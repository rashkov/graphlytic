import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
  val inputFilepath = s"s3a://${bucketName}/wikipedia.dat"
  val outputFilepath = s"s3a://${bucketName}/${timestamp}_wikiTerms.txt"

  // def publishToRedis(){
  //   val r = new RedisClient("10.0.0.8", 6379)
  //   r.set("somekey", "someval");
  //   println(r.get("somekey"))
  // }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(appName)
    val sc = new SparkContext(conf)

    val wikiRdd: RDD[WikipediaArticle] = sc
      .textFile(inputFilepath)
      .map(WikipediaData.parse).cache()

    val wikiTermsPairRdd = wikiRdd
      .flatMap((wiki)=>wiki.terms.map((term)=>(term, wiki.title)))
      .combineByKey(
        (title: String) => Set(title),
        (titles: Set[String], title) => titles+title,
        (titles: Set[String], titles2: Set[String]) => titles++titles2
      )
    wikiTermsPairRdd.saveAsTextFile(outputFilepath)
    // publishToRedis()
 }
}
