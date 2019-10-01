import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import com.databricks.spark.xml._
import com.redis._
import java.time.Instant

object graphlytic {
  val appName = "Graphlytic"
  val timestamp = Instant.now.getEpochSecond.toString
  val bucketName = sys.env.get("bucket") match {
    case Some(name) => name
    case None => throw new Exception("Please define a \"bucket\" environment variable")
  }
  // val inputFilepath = "file:///home/mike/wiki_foo.xml"
  val inputFilepath = s"s3a://${bucketName}/enwiki-20190901-pages-articles-multistream.xml"
  //val inputFilepath = s"s3a://${bucketName}/wikipedia_part.xml"
  val outputFilepath = s"s3a://${bucketName}/${timestamp}_wiki_index"

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._ // Enable $ syntax

    val df = spark.read
      .option("rowTag", "page")
      .xml(inputFilepath)

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("all_terms")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val remover = new StopWordsRemover()
      .setInputCol("all_terms")
      .setOutputCol("filtered_terms")

    val wikiText = df.select("id", "revision.text._VALUE")
      .as[(Long, String)]
      .toDF("id", "text")
      .filter($"id".isNotNull && $"text".isNotNull)
    val wikiTerms = regexTokenizer
      .transform(wikiText)
    val wikiTermsFiltered = remover
      .transform(wikiTerms)
      .drop("text", "all_terms")
      .as[(Long, Array[String])]
      .flatMap { case (id, terms) => terms.map((term)=>(term, id)) }
      .toDF("term", "id")
      .groupBy("term")
      .agg(collect_set(col("id")) as "ids")
      .as[(String, Seq[Long])]
      .write.parquet(outputFilepath)
  }
}
