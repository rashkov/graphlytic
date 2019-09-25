import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import com.databricks.spark.xml._
import wikipedia._
import com.redis._
import java.time.Instant

object graphlytic {
  val appName = "Graphlytic"
  val timestamp = Instant.now.getEpochSecond.toString
  val bucketName = sys.env.get("bucket") match {
    case Some(name) => name
    case None => throw new Exception("Please define a \"bucket\" environment variable")
  }
  // val inputFilepath = s"s3a://${bucketName}/wikipedia_part.xml"
  val inputFilepath = s"s3a://${bucketName}/enwiki-20190901-pages-articles-multistream.xml"
  val outputFilepath = s"s3a://${bucketName}/${timestamp}_wiki_index"

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._ // Enable $ syntax

    val df = spark.read
      .option("rowTag", "page")
      .xml(inputFilepath)

    val wikiTerms = df.select("id", "revision.text")
      .as[(Long, String)]
      .flatMap { case (id, txt) => txt.split(' ').map((term)=>(term, id)) }
      .toDF("term", "id")
      .groupBy("term")
      .agg(array_distinct(collect_list(col("id"))) as "ids")
      .as[(String, Seq[Long])]
      .write.parquet(outputFilepath)
  }
}

// def publishToRedis(){
//   val r = new RedisClient("10.0.0.8", 6379)
//   r.set("somekey", "someval");
//   println(r.get("somekey"))
// }
