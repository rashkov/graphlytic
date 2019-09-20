import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import wikipedia._
import com.redis._

object graphlytic {
  val appName = "Graphlytic"
  val inputFilepath = "hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/wikipedia.dat"
  val outputFilepath = "hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/wikiTerms.txt"
  // def publishToRedis(){
  //   val r = new RedisClient("10.0.0.8", 6379)
  //   r.set("somekey", "someval");
  //   println(r.get("somekey"))
  // }

  def main(args: Array[String]) {
    // setup the Spark Context
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read in the data from HDFS
    val wikiRdd: RDD[WikipediaArticle] = sc
      .textFile(inputFilepath)
      .map(WikipediaData.parse)

    val wikiTermsPairRdd = wikiRdd.flatMap(
      (wiki)=>wiki.terms.map((term)=>(term, wiki.title))
    )
    wikiTermsPairRdd.saveAsTextFile(outputFilepath)
    // publishToRedis()
  }
}
