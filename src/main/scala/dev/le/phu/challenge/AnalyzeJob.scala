package phu.le.dev.challenge

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import scala.util.matching.Regex

object AnalyzeJob {
	//	Period of user inactivity for requests from the same user are considered a second session.
	// final val INACTIVITY_THRESHOLD: String = "30 minutes"
	final val INACTIVITY_THRESHOLD: String = "1 hour"
	case class Record(time: String, clientAddress: String, requestURL: String)

	def main(args: Array[String]): Unit = {
		if (args.length < 1) {
			System.err.println("Usage: AnalyzeJob <input-file> <output-dir>")
			System.exit(1)
		}
		val inputFile = args(0)
		var outputDir = args(1)

		val spark = SparkSession
			.builder
			.appName("Analyze Job")
			.getOrCreate()

		import spark.implicits._
		/* Sample
		+--------------------------+---------------+-----------------------------------------------------------------------+
		|time                      |clientAddress  |requestURL                                                             |
		+--------------------------+---------------+-----------------------------------------------------------------------+
		|2015-07-22 09:00:28.019143|123.242.248.130|https://paytm.com:443/shop/authresponse                                |
		|2015-07-22 09:00:27.89458 |203.91.211.44  |https://paytm.com:443/shop/wallet/txnhistory                           |
		|2015-07-22 09:00:27.885745|1.39.32.179    |https://paytm.com:443/shop/wallet/txnhistory                           |
		+--------------------------+---------------+-----------------------------------------------------------------------+
		*/
		val webLogDF = readInputFile(spark, inputFile)
		webLogDF.printSchema()
    	webLogDF.show(10, false)

		val sessioniedDF = runSessionizedDF(webLogDF)
		sessioniedDF.printSchema()
    	sessioniedDF.show(10, false)

		val uniqPageHitDF = runUniqPageHitDF(sessioniedDF)
		uniqPageHitDF.printSchema()
		uniqPageHitDF.show(10, false)
		outputCSV(
			uniqPageHitDF
				.withColumn("session", to_json($"session"))
				.withColumn("uniqPageHit", to_json($"uniqPageHit")),
			s"file://${outputDir}/uniqPageHit.csv"
		)


		/*	Sample:
		+-------------+------------------------------------------+--------------------------+--------------------------+---------------------------+------------------+
		|clientAddress|session                                   |firstPageHit              |lastPageHit               |sessionTimeInterval        |sessionTimeSeconds|
		+-------------+------------------------------------------+--------------------------+--------------------------+---------------------------+------------------+
		|1.187.50.18  |{2015-07-22 16:00:00, 2015-07-22 17:00:00}|2015-07-22 16:24:45.630977|2015-07-22 16:24:50.244793|4.613816 seconds           |5                 |
		|1.22.128.88  |{2015-07-22 16:00:00, 2015-07-22 17:00:00}|2015-07-22 16:20:33.723461|2015-07-22 16:25:02.779017|4 minutes 29.055556 seconds|269               |
		|1.22.196.102 |{2015-07-22 16:00:00, 2015-07-22 17:00:00}|2015-07-22 16:42:34.770762|2015-07-22 16:43:52.207071|1 minutes 17.436309 seconds|78                |
		|1.23.195.20  |{2015-07-22 17:00:00, 2015-07-22 18:00:00}|2015-07-22 17:42:14.732586|2015-07-22 17:45:01.514754|2 minutes 46.782168 seconds|167               |
		|1.38.19.17   |{2015-07-22 16:00:00, 2015-07-22 17:00:00}|2015-07-22 16:43:28.528764|2015-07-22 16:43:28.528764|0 seconds                  |0                 |
		+-------------+------------------------------------------+--------------------------+--------------------------+---------------------------+------------------+
		*/
		val sessionTimeIncludedDF = runSessionTimeIncludedDF(spark, sessioniedDF)
		sessionTimeIncludedDF.printSchema()
    	sessionTimeIncludedDF.show(10, false)
		outputCSV(
			sessionTimeIncludedDF
				.withColumn("session", to_json($"session"))
				.withColumn("sessionTimeInterval", $"sessionTimeInterval".cast(StringType))
				.withColumn("firstPageHit", $"firstPageHit".cast(StringType))
				.withColumn("lastPageHit", $"lastPageHit".cast(StringType)),
			s"file://${outputDir}/sessionTimeIncluded.csv"
		)

		/*	Sample:
		+---------------+---------------------+
		|clientAddress  |maxSessionTimeSeconds|
		+---------------+---------------------+
		|220.226.206.7  |3558                 |
		|180.151.80.140 |3300                 |
		|121.58.175.128 |2060                 |
		+---------------+---------------------+
		*/
		val mostEngagedUserDF = runMostEngagedUserDF(spark, sessionTimeIncludedDF)
		mostEngagedUserDF.printSchema()
    	mostEngagedUserDF.show(10, false)
		outputCSV(mostEngagedUserDF, s"file://${outputDir}/mostEngagedUser.csv")
		/*
		Sample:
		+---------------+-------------------------+
		|clientAddress  |averageSessionTimeSeconds|
		+---------------+-------------------------+
		|223.182.246.141|2060                     |
		|14.97.148.248  |2058                     |
		|8.37.224.151   |2053                     |
		+---------------+-------------------------+
		*/
		val avgSessionTimeDF = runAverageSessionTimeDF(spark, sessionTimeIncludedDF)
		avgSessionTimeDF.printSchema()
    	avgSessionTimeDF.show(10, false)
		outputCSV(avgSessionTimeDF, s"file://${outputDir}/avgSessionTime.csv")

		spark.stop()
	}


	/** Read Access Log from Input File
	*	- 1> Extract these entries
	*		- Time
	*		- Client IP Address
	*		- Request URL
	*	- 2> In my opinion, we should remove requests to Asset Files such as: css, js, png, jpg etc
	*	- 3> Convert time string to timestamp type
	*
	*	@param inputFile Absolute Path to Input File
	*/
	private def readInputFile(spark: SparkSession, inputFile: String): DataFrame = {
		import spark.implicits._
		return spark.sparkContext.textFile(inputFile)
			.map(_.split(" "))
			.map(parts => Record(parts{0}, parts{2}.split(":"){0}, parts{12}.split("\\?"){0}))
			.filter(r => !(r.requestURL.endsWith(".css")))
			.filter(r => !(r.requestURL.endsWith(".js")))
			.filter(r => !(r.requestURL.endsWith(".png")))
			.filter(r => !(r.requestURL.endsWith(".jpg")))
			.filter(r => !(r.requestURL.endsWith(".jpeg")))
			.filter(r => !(r.requestURL.endsWith(".svg")))
			.filter(r => !(r.requestURL.endsWith(".ico")))
			.toDF()
			.withColumn("time", to_timestamp(col("time")))
	}

	/** Group by Access Log Entries by
	*	- Client IP Address
	*	- Time based on Window (Default: 1 Hour) (New Column: `session`)
	*	Then I collected all entries in the session window into an array of struct format (New Column: `pageHits`)
	*		- time
	*		- requestURL
	*
	*	@param df Dataframe
	*	@param inactivityThreshold Session Inactivity Threshold
	*/
	def runSessionizedDF(df: DataFrame, inactivityThreshold: String = INACTIVITY_THRESHOLD): DataFrame = {
		return df
			.groupBy(col("clientAddress"), window(col("time"), inactivityThreshold).alias("session"))
			.agg(sort_array(collect_list(struct(col("time"), col("requestURL")))).alias("pageHits"))
	}


	/** Extract Unique Requestl URL and count
	*	- 1> Use `array_distinct` to find unique Request URL
	*	- 2> Use `size` to count unique Request URL
	*
	*	@param sessionizedDF Sessionized DataFrame
	*/
	def runUniqPageHitDF(sessionizedDF: DataFrame): DataFrame = {
		return sessionizedDF
			.select(
				col("clientAddress"),
				col("session"),
				col("pageHits")
			)
			.withColumn("uniqPageHit", array_distinct(col("pageHits.requestURL")))
			.withColumn("uniqPageHitCount", size(col("uniqPageHit")))
			.drop(col("pageHits"))
	}

	/** Grouping PageHits and calculate Session Time
	*	- 1> Get Time of First Page Hit in each session
	*	- 2> Get Time of Last Page Hit in each session
	*	- 3> Subtract the last Page Hit event to the first Page Hit in each session Window
	*	- 4> Convert SessionTimeInterval to SessionTimeSeconds
	*	@param spark
	*	@param sessionizedDF Sessionized DataFrame
	*/
	def runSessionTimeIncludedDF(spark: SparkSession, sessionizedDF: DataFrame): DataFrame = {
		import spark.implicits._
		return sessionizedDF
			.withColumn("firstPageHit", $"pageHits".apply(0).getItem("time"))
			.withColumn("lastPageHit", $"pageHits".apply(size($"pageHits").minus(1)).getItem("time"))
			.withColumn("sessionTimeInterval", $"lastPageHit" - $"firstPageHit")
			.withColumn("sessionTimeSeconds", $"lastPageHit".cast(LongType) - $"firstPageHit".cast(LongType))
			.drop(col("pageHits"))
	}

	/** Find Most Engaged Users = IPs with the longest session times
	*	- 1> Group By Client IP Address
	*	- 2> Aggregate to find max session time per IPs
	*	- 3> Sort
	*	@param spark
	*	@param sessionTimeIncludedDF Sessionized DataFrame with Session Time Calculated
	*/
	def runMostEngagedUserDF(spark: SparkSession, sessionTimeIncludedDF: DataFrame): DataFrame = {
		import spark.implicits._
		return sessionTimeIncludedDF
			.select($"clientAddress", $"sessionTimeSeconds")
			.groupBy($"clientAddress")
			.agg(max(col("sessionTimeSeconds")).alias("maxSessionTimeSeconds"))
			.sort(desc("maxSessionTimeSeconds"))
	}

	/** Find Average Session Times per IP
	*	- 1> Group By Client IP Address
	*	- 2> Aggregate to calculate avg session time per IP and round half up
	*	- 3> Sort
	*	@param spark
	*	@param sessionTimeIncludedDF Sessionized DataFrame with Session Time Calculated
	*/
	def runAverageSessionTimeDF(spark: SparkSession, sessionTimeIncludedDF: DataFrame): DataFrame = {
		import spark.implicits._
		return sessionTimeIncludedDF
			.select($"clientAddress", $"sessionTimeSeconds")
			.groupBy($"clientAddress")
			.agg(round(avg($"sessionTimeSeconds")).cast(LongType).alias("averageSessionTimeSeconds"))
			.sort(desc("averageSessionTimeSeconds"))
	}

	private def outputCSV(df: DataFrame, filePath: String) {
		df
			.coalesce(1) // Force write only a single file, because I do not use HDFS
			.write
			.option("header", true)
			.mode(SaveMode.Overwrite)
			.csv(filePath)
	}
}

