import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import com.databricks.spark.xml._
import java.time.Instant

object graphlytic {
  val timestamp = Instant.now.getEpochSecond.toString
  val bucketName = sys.env.get("bucket") match {
    case Some(name) => name
    case None => throw new Exception("Please define a \"bucket\" environment variable")
  }
  val inputFilepath = s"s3a://${bucketName}/enwiki-20190901-pages-articles-multistream.xml"
  val outputFilepath = s"s3a://${bucketName}/${timestamp}_wiki_index"

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._ // Enable $ syntax among other things

    // Parse input XML by reading <page> tags
    val df = spark.read
      .option("rowTag", "page")
      .xml(inputFilepath)

    // Split words using regexTokenizer
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("all_terms")
      .setPattern("\\W")

    // Filter out english stopwords
    val remover = new StopWordsRemover()
      .setInputCol("all_terms")
      .setOutputCol("filtered_terms")

    // Select article ID & text fields, remove any with null values
    val wikiText = df.select("id", "revision.text._VALUE")
      .as[(Long, String)]
      .toDF("id", "text")
      .filter($"id".isNotNull && $"text".isNotNull)

    // Tokenize
    val wikiTerms = regexTokenizer
      .transform(wikiText)

    // Remove stopwords and build index, then write it to parquet in S3
    val wikiTermsFiltered = remover
      .transform(wikiTerms)
      .drop("text", "all_terms")
      .as[(Long, Array[String])]
      .flatMap { case (id, terms) => terms.map((term)=>(term, id)) }
      .toDF("term", "id")
      .groupBy("term")
      .agg(collect_set(col("id")) as "ids") // De-duplicate IDs via collect_set
      .as[(String, Seq[Long])]
      .write.parquet(outputFilepath)
  }
}
