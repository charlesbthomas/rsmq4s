import RedisSMQ.redisFutureToFuture
import io.lettuce.core.cluster.{RedisClusterClient, SlotHash}
import io.lettuce.core.{AbstractRedisClient, RedisClient, RedisFuture}

import scala.compat.java8.FutureConverters.{CompletionStageOps, toScala}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.ScriptOutputType
import java.time.OffsetDateTime
import io.lettuce.core
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import java.nio.ByteBuffer
import java.util.ArrayList

case class RedisSMQConfig(
    redisClient: Either[RedisClient, RedisClusterClient],
    namespace: String = "rsmq"
)

case class RedisQueue(
    vt: Long,
    delay: Long,
    maxsize: Long,
    ts: Long
)

case class MessageResult(
    id: String,
    message: String,
    rc: Long,
    fr: OffsetDateTime
)

case class ChangeMessageVisibilityOptions(qname: String, id: String, vt: Long)

case class CreateQueueOptions(
    qname: String,
    vt: Long = 30,
    delay: Long = 0,
    maxsize: Long = 65536
)

case class RedisQueueAttributes(
    qname: String,
    vt: Long,
    delay: Long,
    maxsize: Long,
    totalReceived: Long,
    totalSent: Long,
    created: OffsetDateTime,
    modified: OffsetDateTime,
    numMsgs: Long,
    numHiddenMsgs: Long
)

class RedisSMQ(
    config: RedisSMQConfig
)(implicit val executionContext: ExecutionContext) {

  // private val redis = config.redisClient.fold(
  //   redisClient => redisClient.connect().async(),
  //   redisClusterClient => redisClusterClient.connect().async()
  // )

  private val ns = config.namespace

  def sendMessage(
      qname: String,
      message: String,
      delay: Long = 0
  ): Future[String] = {

    val key = s"{$ns:$qname}"
    val qCommands = getRedisCommands(key)

    val uid = UUID.randomUUID().toString()

    for {
      q <- this.getQueue(qname)
      _ = qCommands.multi()
      addFt = qCommands.zadd(key, q.ts + (delay * 1000), uid)
      setFt = qCommands.hset(s"$key:Q", uid, message)
      incFt = qCommands.hincrby(s"$key:Q", "totalsend", 1)
      _ <- qCommands.exec()
      _ <- addFt
      _ <- setFt
      _ <- incFt
    } yield uid
  }

  def popMessage(qname: String): Future[MessageResult] = {
    val key = s"{$ns:$qname}"
    val qCommands = getRedisCommands(key)

    for {
      q <- this.getQueue(qname)
      evalResult <- qCommands
        .eval[ArrayList[Any]](
          RedisSMQScripts.PopMessage,
          ScriptOutputType.MULTI,
          key,
          q.ts.toString
        )
      msg = MessageResult(
        evalResult.get(0).asInstanceOf[String],
        evalResult.get(1).asInstanceOf[String],
        evalResult.get(2).asInstanceOf[Long],
        OffsetDateTime.ofInstant(
          Instant.ofEpochMilli(
            evalResult.get(3).asInstanceOf[String].toLong * 1000
          ),
          ZoneId.systemDefault()
        )
      )
    } yield msg
  }

  def receiveMessage(qname: String): Future[MessageResult] = {
    val key = s"{$ns:$qname}"
    val qCommands = getRedisCommands(key)

    for {
      q <- this.getQueue(qname)
      evalResult <- qCommands
        .eval[ArrayList[Any]](
          RedisSMQScripts.ReceiveMessage,
          ScriptOutputType.MULTI,
          key,
          q.ts.toString,
          (q.ts + (q.vt * 1000)).toString()
        )
      msg = MessageResult(
        evalResult.get(0).asInstanceOf[String],
        evalResult.get(1).asInstanceOf[String],
        evalResult.get(2).asInstanceOf[Long],
        OffsetDateTime.ofInstant(
          Instant.ofEpochMilli(
            evalResult.get(3).asInstanceOf[String].toLong * 1000
          ),
          ZoneId.systemDefault()
        )
      )
    } yield msg
  }

  // def listQueues: Future[Set[String]] = {
  //   redis.smembers(s"$ns:").map(_.asScala.toSet)
  // }

  def createQueue(options: CreateQueueOptions): Future[Long] = {

    val key = s"{$ns:${options.qname}}:Q"
    val commands = getRedisCommands(key)
    val nsCommands = getRedisCommands(s"{$ns}:QUEUES")

    for {
      time <- commands.time()
      _ = commands.multi()
      vtFt = commands.hsetnx(key, "vt", options.vt.toString())
      delayFt = commands.hsetnx(key, "delay", options.delay.toString())
      createdFt = commands.hsetnx(key, "created", time.get(0))
      maxSizeFt = commands.hsetnx(key, "maxsize", options.maxsize.toString)
      modFt = commands.hsetnx(key, "modified", time.get(0))
      exec <- commands.exec()
      vt <- vtFt
      delay <- delayFt
      created <- createdFt
      mod <- modFt
      _ <- nsCommands.sadd(s"{$ns}:QUEUES", options.qname)
    } yield 1L
  }

  def deleteQueue(qname: String): Future[Unit] = {
    val key = s"{$ns:$qname}:Q"
    val qCommands = getRedisCommands(key)
    val nsCommands = getRedisCommands(s"{$ns}:QUEUES")

    for {
      _ <- qCommands.del(key)
      _ <- nsCommands.srem(s"{$ns}:QUEUES")
    } yield ()
  }

  def getQueueAttributes(qname: String): Future[RedisQueueAttributes] = {
    val key = s"{$ns:$qname}"
    val qCommands = getRedisCommands(key)
    qCommands.multi()
    for {
      time <- qCommands.time()
      _ = qCommands.multi()
      qAttFt = qCommands.hmget(
        s"$key:Q",
        "vt",
        "delay",
        "maxsize",
        "totalrecv",
        "totalsent",
        "created",
        "modified"
      )
      allMsgsFt = qCommands.zcard(key)
      hiddenMsgsFt = qCommands.zcount(key, time.get(0) + "000", "+inf")
      _ <- qCommands.exec()
      qAtt <- qAttFt
      allMsgs <- allMsgsFt
      hiddenMsgs <- hiddenMsgsFt
    } yield RedisQueueAttributes(
      qname,
      qAtt.get(0).getValue.toLong,
      qAtt.get(1).getValue.toLong,
      qAtt.get(2).getValue.toLong,
      qAtt.get(3).getValue.toLong,
      qAtt.get(4).getValue.toLong,
      OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(qAtt.get(5).getValue.toLong),
        ZoneId.systemDefault()
      ),
      OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(qAtt.get(6).getValue.toLong),
        ZoneId.systemDefault()
      ),
      allMsgs,
      hiddenMsgs
    )
  }

  def deleteMessage(id: String, qname: String): Future[Boolean] = {
    val qKey = s"{$ns:$qname}"
    val qCommands = getRedisCommands(qname)

    qCommands.multi()
    val rmFt = qCommands.zrem(qKey, id)
    val delFt = qCommands.hdel(qKey, id, s"$id:rc", s"$id:fr")
    for {
      _ <- qCommands.exec()
      rm <- rmFt
      del <- delFt
    } yield rm == 1 && del > 0
  }

  private def getQueue(qname: String): Future[RedisQueue] = {
    val queueKey = s"{$ns:$qname}:Q"
    val redis = config.redisClient.fold(
      _.connect().async(),
      cluster => {
        val node =
          cluster.getPartitions.getPartitionBySlot(SlotHash.getSlot(queueKey))
        cluster.connect().getConnection(node.getNodeId).async()
      }
    )

    val txn = redis.multi()
    val queue = redis.hmget(queueKey, "vt", "delay", "maxsize").toScala
    val serverTime = redis.time().toScala

    for {
      r <- redis.exec()
      q <- queue
      ts <- serverTime
      _ = println(ts)
      _ <- txn
    } yield RedisQueue(
      q.get(0).getValue.toLong,
      q.get(1).getValue().toLong,
      q.get(2).getValue().toLong,
      ts.get(0).toLong
    )
  }

  private def changeMessageVisibility(
      options: ChangeMessageVisibilityOptions
  ): Future[Long] = {
    val ChangeMessageVisibilityOptions(qname, id, vt) = options
    val redis = getRedisCommands(qname)
    for {
      q <- getQueue(qname)
      res <- redis.eval[Long](
        RedisSMQScripts.ChangeMessageVisibility,
        ScriptOutputType.INTEGER,
        s"{$ns:$qname}",
        id,
        (q.ts + vt * 1000).toString()
      )
    } yield res
  }

  // private def initScript(script: String): Future[String] = {
  //   redis.scriptLoad(script).toScala
  // }

  private def getRedisCommands(key: String) =
    config.redisClient.fold(
      _.connect().async(),
      cluster => {
        val node =
          cluster.getPartitions.getPartitionBySlot(SlotHash.getSlot(key))
        cluster.connect().getConnection(node.getNodeId).async()
      }
    )
}

object RedisSMQ {
  implicit def redisFutureToFuture[A](rf: RedisFuture[A]): Future[A] =
    rf.toScala
}

object RedisSMQScripts {

  // The popMessage LUA Script
  //
  // Parameters:
  //
  // KEYS[1]: the zset key
  // KEYS[2]: the current time in ms
  //
  // * Find a message id
  // * Get the message
  // * Increase the rc (receive count)
  // * Use hset to set the fr (first receive) time
  // * Return the message and the counters
  //
  // Returns:
  //
  // {id, message, rc, fr}
  val PopMessage =
    s"""
       |  local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
       |  if #msg == 0 then
       |    return {}
       |  end
       |  redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
       |  local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
       |  local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
       |  local o = {msg[1], mbody, rc}
       |  if rc==1 then
       |    table.insert(o, KEYS[2])
       |  else
       |    local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
       |  table.insert(o, fr)
       |  end
       |  redis.call("ZREM", KEYS[1], msg[1])
       |  redis.call("HDEL", KEYS[1] .. ":Q", msg[1], msg[1] .. ":rc", msg[1] .. ":fr")
       |  return o
       |""".stripMargin

  // The receiveMessage LUA Script
  //
  // Parameters:
  //
  // KEYS[1]: the zset key
  // KEYS[2]: the current time in ms
  // KEYS[3]: the new calculated time when the vt runs out
  //
  // * Find a message id
  // * Get the message
  // * Increase the rc (receive count)
  // * Use hset to set the fr (first receive) time
  // * Return the message and the counters
  //
  // Returns:
  //
  // {id, message, rc, fr}
  val ReceiveMessage =
    s"""
      | local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
      | if #msg == 0 then
      |   return {}
      |	end
      |	redis.call("ZADD", KEYS[1], KEYS[3], msg[1])
      |	redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
      |	local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
      |	local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
      |	local o = {msg[1], mbody, rc}
      |	if rc==1 then
      |	  redis.call("HSET", KEYS[1] .. ":Q", msg[1] .. ":fr", KEYS[2])
      |	  table.insert(o, KEYS[2])
      |	else
      |	  local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
      |   table.insert(o, fr)
      | end
      | return o
      |""".stripMargin

  // The changeMessageVisibility LUA Script
  //
  // Parameters:
  //
  // KEYS[1]: the zset key
  // KEYS[2]: the message id
  //
  //
  // * Find the message id
  // * Set the new timer
  //
  // Returns:
  //
  // 0 or 1
  val ChangeMessageVisibility =
    s"""
       |  local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])
       |  if not msg then
       |    return 0
       |  end
       |  redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])
       |  return 1
       |""".stripMargin
}
