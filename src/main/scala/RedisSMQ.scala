import RedisSMQ.redisFutureToFuture
import io.lettuce.core.cluster.{RedisClusterClient, SlotHash}
import io.lettuce.core.{AbstractRedisClient, RedisClient, RedisFuture}

import scala.compat.java8.FutureConverters.{CompletionStageOps, toScala}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

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

class RedisSMQ(
    config: RedisSMQConfig,
    implicit val executionContext: ExecutionContext
) {

  private val redis = config.redisClient.fold(
    redisClient => redisClient.connect().async(),
    redisClusterClient => redisClusterClient.connect().async()
  )

  private val ns = config.namespace

  lazy val popMessageSha1 = initScript(RedisSMQScripts.PopMessage)
  lazy val receiveMessageSha1 = initScript(RedisSMQScripts.ReceiveMessage)
  lazy val changeMessageVisibilitySha1 = initScript(
    RedisSMQScripts.ChangeMessageVisibility
  )

  def listQueues: Future[Set[String]] = {
    redis.smembers(s"$ns:").map(_.asScala.toSet)
  }

  private def _getQueue(qname: String): Future[RedisQueue] = {
    val queueKey = s"{$ns:$qname}:Q"
    val redis = config.redisClient.fold(
      _.connect().async(),
      cluster => {
        val node =
          cluster.getPartitions.getPartitionBySlot(SlotHash.getSlot(queueKey))
        cluster.connect().getConnection(node.getNodeId).async()
      }
    )

    redis.multi()
    val queue = redis.hmget(queueKey, "vt", "delay", "maxsize").toScala
    val serverTime = redis.time().toScala

    val q = for {
      r <- redis.exec()
      q <- queue
      ts <- serverTime
    } yield RedisQueue(q.get(0).getValue.toLong, q.get(1)



    for {
      txn <- redis.multi()
      _ <- txn.
    } yield ()

  }

  private def initScript(script: String): Future[String] = {
    redis.scriptLoad(script).toScala
  }

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
