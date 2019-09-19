import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import wikipedia._

object graphlytic {
 def main(args: Array[String]) {

   // setup the Spark Context
   val conf = new SparkConf().setAppName("Wiki").setMaster("local[2]")
   val sc = new SparkContext(conf)

   // read in the data from HDFS
   val inputFilepath = "hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/wikipedia.dat"
   val outputFilepath = "hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/wikiTerms.txt"
   val wikiRdd: RDD[WikipediaArticle] = sc
     .textFile(inputFilepath)
     .map(WikipediaData.parse)

   val wikiTermsPairRdd = wikiRdd.flatMap(
     (wiki)=>wiki.terms.map((term)=>(term, wiki.title))
   )
   wikiTermsPairRdd.saveAsTextFile(outputFilepath)

   //price_vol_min30.saveAsTextFile("hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/price_data_output_scala")

   // // map each record into a tuple consisting of (time, price, volume)
   // val ticks = file.map(line => {
   //                      val record = line.split(";")
   //                     (record(0), record(1).toDouble, record(2).toInt)
   //                              })

   // // apply the time conversion to the time portion of each tuple and persist it memory for later use
   // val ticks_min30 = ticks.map(record => (convert_to_30min(record._1),
   //                                        record._2,
   //                                        record._3)).persist

   // // compute the average price for each 30 minute period
   // val price_min30 = ticks_min30.map(record => (record._1, (record._2, 1)))
   //                              .reduceByKey( (x, y) => (x._1 + y._1,
   //                                                       x._2 + y._2) )
   //                              .map(record => (record._1,
   //                                              record._2._1/record._2._2) )

   // // compute the total volume for each 30 minute period
   // val vol_min30 = ticks_min30.map(record => (record._1, record._3))
   //                            .reduceByKey(_+_)

   // // join the two RDDs into a new RDD containing tuples of (30 minute time periods, average price, total volume)
   // val price_vol_min30 = price_min30.join(vol_min30)
   //                                  .sortByKey()
   //                                  .map(record => (record._1,
   //                                                  record._2._1,
   //                                                  record._2._2))

   // save the data back into HDFS
   //price_vol_min30.saveAsTextFile("hdfs://ec2-3-212-99-96.compute-1.amazonaws.com:9000/user/price_data_output_scala")
 }
}
