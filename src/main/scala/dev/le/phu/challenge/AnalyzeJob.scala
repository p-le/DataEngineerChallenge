package phu.le.dev.challenge

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import scala.util.matching.Regex

object AnalyzeJob extends App {
	if (args.length < 1) {
		System.err.println("Usage: AnalyzeJob <input-file>")
		System.exit(1)
	}
	val inputFile = args(0)
	val spark = SparkSession
		.builder
		.appName("Analyze Job")
		.getOrCreate()

	import spark.implicits._

	case class Record(time: String, clientAddress: String, requestURL: String)

	var webLogRDD = spark.sparkContext.textFile(inputFile)
	val webLogDF = webLogRDD
		.map(_.split(" "))
		.map(parts => Record(parts{0}, parts{2}.split(":"){0}, parts{12}.split("\\?"){0}))
		.filter(r => !(r.requestURL.endsWith(".css")))
    	.filter(r => !(r.requestURL.endsWith(".js")))
		.filter(r => !(r.requestURL.endsWith(".png")))
    	.filter(r => !(r.requestURL.endsWith(".jpg")))
		.filter(r => !(r.requestURL.endsWith(".jpeg")))
		.filter(r => !(r.requestURL.endsWith(".svg")))
    	.filter(r => !(r.requestURL.endsWith(".ico")))
		.withColumn("datetime", to_timestamp(col("datetime")))
		.toDF()

	webLogDF.printSchema()
    webLogDF.show(10, false)

	spark.stop()
}
