package example

import org.apache.spark.sql.SparkSession
import com.redis._

object Hello extends App {
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._ // Enable implicit conversion in .as[]
  val bucketPath = sys.env.get("BUCKET_PATH") match {
    case Some(path) => path
    case None => throw new Exception("Please define a \"BUCKET_PATH\" environment variable")
  }

  val df = spark.read.parquet("s3a://" + bucketPath)
  df
    .as[(String, Seq[Long])]
    .foreachPartition((part)=>{
      val redisHost = sys.env.get("REDIS_HOST") match {
        case Some(host) => host.toString
        case None => throw new Exception("Please define a \"REDIS_HOST\" environment variable")
      }
      val redisPort = sys.env.get("REDIS_PORT") match {
        case Some(port) => port.toInt
        case None => 6379
      }
      val r = new RedisClientPool(redisHost, redisPort)
      r.withClient {
        client => {
          part.foreach((row)=>{
            val (term, ids) = row
            val limitIds = ids.take(10000)
            client.sadd(term, limitIds.head, limitIds.tail: _*)
          })
        }
      }
    })
}
