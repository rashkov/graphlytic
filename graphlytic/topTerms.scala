import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, IDF, CountVectorizer, CountVectorizerModel}
import com.databricks.spark.xml._
import java.time.Instant

val timestamp = Instant.now.getEpochSecond.toString
val bucketName = sys.env.get("bucket") match {
  case Some(name) => name
  case None => throw new Exception("Please define a \"bucket\" environment variable")
}
// val inputFilepath = s"s3a://${bucketName}/enwiki-20190901-pages-articles-multistream.xml"
val inputFilepath = s"s3a://${bucketName}/wikipedia_part.xml"
val outputFilepath = s"s3a://${bucketName}/${timestamp}_wiki_index"

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
  .where($"filtered_terms"(0) =!= "redirect")
  .drop("text", "all_terms")
  .as[(Long, Array[String])]

val cvModel: CountVectorizerModel = new CountVectorizer()
  .setInputCol("filtered_terms")
  .setOutputCol("rawFeatures")
  .setMinDF(2)
  .fit(wikiTermsFiltered)

val wikiTermsFeaturized= cvModel
  .transform(wikiTermsFiltered)

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(wikiTermsFeaturized)

val rescaledData = idfModel.transform(wikiTermsFeaturized)

def topTermsUdf = udf((terms: org.apache.spark.ml.linalg.SparseVector)=>{
  (terms.indices zip terms.values)
    .sortBy(_._2)(Ordering[Double].reverse)
    .take(100)
    .toArray
    .map{
      case (wordIdx: Int, score: Double) => (cvModel.vocabulary(wordIdx), score)
    }
})
rescaledData.withColumn("topTerms", topTermsUdf($"features")).show

def camelToArray(name: String) =
  "[A-Z\\d]".r.replaceAllIn(
    name,
    {m => " " + m.group(0).toLowerCase()}
  )
  .split(' ')
  .filter(_.nonEmpty)

// val getTopTerms: (org.apache.spark.ml.linalg.SparseVector)=>Array[(String, Double)] = (terms: org.apache.spark.ml.linalg.SparseVector)=>{
//   (terms.indices zip terms.values)
//     .sortBy(_._2)(Ordering[Double].reverse)
//     .take(100)
//     .toArray
//     .map{
//       case (wordIdx: Int, score: Double) => (cvModel.vocabulary(wordIdx), score)
//     }
// }
// def fooUdf = udf((terms: org.apache.spark.ml.linalg.SparseVector)=>{
//   (terms.indices zip terms.values)
//     .sortBy(_._2)(Ordering[Double].reverse)
//     .take(100)
//     .toArray
//     .map{
//       case (wordIdx: Int, score: Double) => (cvModel.vocabulary(wordIdx), score)
//     }
// })
// val newCol = udf(getTopTerms).apply($"features")
// rescaledData.withColumn("topTerms", newCol).show
