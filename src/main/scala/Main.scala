import io.lettuce.core.RedisClient
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {

  val client = RedisClient.create("redis://localhost/")
  val conf = RedisSMQConfig(Left(client), "test")

  val q = new RedisSMQ(conf)

  val opts = CreateQueueOptions("my_queue")

  Await.result(
    for {
      _ <- q.createQueue(opts)
      uid <- q.sendMessage("my_queue", "Hello World")
      foo <- q.receiveMessage("my_queue")
    } yield uid,
    30.seconds
  )

}
