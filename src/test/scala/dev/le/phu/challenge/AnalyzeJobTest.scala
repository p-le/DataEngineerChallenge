package phu.le.dev.challenge

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp

class AnalyzeJobTest extends AnyFunSuite with BeforeAndAfter {

	var spark: SparkSession = _

	before {
		spark = SparkSession
			.builder
			.master("local[2]")
			.appName("Test Analyze Job")
			.getOrCreate()
  	}

    test("Analyze Job Test Sessionized DF") {
		val spark2 = spark
		import spark2.implicits._

		val expectedSchema = new StructType()
			.add("clientAddress",StringType)
			.add("session", new StructType()
				.add("start", TimestampType)
				.add("end", TimestampType), false)
			.add("pageHits", ArrayType(
				new StructType()
					.add("time", TimestampType)
					.add("requestURL", StringType),
				false
			), false)

		val expectedData = Seq(
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 10:00:00"), Timestamp.valueOf("2015-07-22 11:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 10:39:38.707446"),"https://paytm.com:443/shop/cart?channel=web&version=2"),
					Row(Timestamp.valueOf("2015-07-22 10:45:53.245587"),"https://paytm.com:443/shop/p/mesleep-best-brother-wooden-coaster-set-of-6-HOMMESLEEP-BESTME-S111A438555B")
				)
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 11:00:00"), Timestamp.valueOf("2015-07-22 12:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 11:00:45.860139"),"https://www.paytm.com:443/blog/addpaytmcashfrommobile"),
					Row(Timestamp.valueOf("2015-07-22 11:55:45.451693"),"https://paytm.com:443/papi/rr/products/14068587/statistics?channel=web&version=2")
				)
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 09:00:00"), Timestamp.valueOf("2015-07-22 10:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 09:02:31.935289"),"https://paytm.com:443/papi/nps/merchantrating?merchant_id=28273&channel=web&version=2"),
					Row(Timestamp.valueOf("2015-07-22 09:03:56.860666"),"https://paytm.com:443/shop/cart?channel=web&version=2")
				)
			)
    	)

		val expectedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(expectedData),
      		expectedSchema
    	)

		val data = Seq(
			("2015-07-22T09:02:31.935289Z", "180.151.80.140", "https://paytm.com:443/papi/nps/merchantrating?merchant_id=28273&channel=web&version=2"),
			("2015-07-22T09:03:56.860666Z", "180.151.80.140", "https://paytm.com:443/shop/cart?channel=web&version=2"),
			("2015-07-22T10:45:53.245587Z", "180.151.80.140", "https://paytm.com:443/shop/p/mesleep-best-brother-wooden-coaster-set-of-6-HOMMESLEEP-BESTME-S111A438555B"),
			("2015-07-22T10:39:38.707446Z", "180.151.80.140", "https://paytm.com:443/shop/cart?channel=web&version=2"),
			("2015-07-22T11:00:45.860139Z", "180.151.80.140", "https://www.paytm.com:443/blog/addpaytmcashfrommobile"),
			("2015-07-22T11:55:45.451693Z", "180.151.80.140", "https://paytm.com:443/papi/rr/products/14068587/statistics?channel=web&version=2"),
		)

		val rdd = spark.sparkContext.parallelize(data)
		val df = rdd.toDF("time","clientAddress", "requestURL").withColumn("time", to_timestamp($"time"))
		val outputDF = AnalyzeJob.runSessionizedDF(df)

		assertResult(expectedDF.collect())(AnalyzeJob.runSessionizedDF(df).collect())
    }

	test("Analyze Job Test Session Time Included DF") {
		val sessionizedDFSchema = new StructType()
			.add("clientAddress",StringType)
			.add("session", new StructType()
				.add("start", TimestampType)
				.add("end", TimestampType), false)
			.add("pageHits", ArrayType(
				new StructType()
					.add("time", TimestampType)
					.add("requestURL", StringType),
				false
			), false)

		val sessionizedDFData = Seq(
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 10:00:00"), Timestamp.valueOf("2015-07-22 11:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 10:39:38.707446"),"https://paytm.com:443/shop/cart?channel=web&version=2"),
					Row(Timestamp.valueOf("2015-07-22 10:45:53.245587"),"https://paytm.com:443/shop/p/mesleep-best-brother-wooden-coaster-set-of-6-HOMMESLEEP-BESTME-S111A438555B")
				)
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 11:00:00"), Timestamp.valueOf("2015-07-22 12:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 11:00:45.860139"),"https://www.paytm.com:443/blog/addpaytmcashfrommobile"),
					Row(Timestamp.valueOf("2015-07-22 11:55:45.451693"),"https://paytm.com:443/papi/rr/products/14068587/statistics?channel=web&version=2")
				)
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 09:00:00"), Timestamp.valueOf("2015-07-22 10:00:00")),
				List(
					Row(Timestamp.valueOf("2015-07-22 09:02:31.935289"),"https://paytm.com:443/papi/nps/merchantrating?merchant_id=28273&channel=web&version=2"),
					Row(Timestamp.valueOf("2015-07-22 09:03:56.860666"),"https://paytm.com:443/shop/cart?channel=web&version=2")
				)
			)
    	)

		val sessionizedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(sessionizedDFData),
      		sessionizedDFSchema
    	)

		val expectedSchema = new StructType()
			.add("clientAddress",StringType)
			.add("session", new StructType()
				.add("start", TimestampType)
				.add("end", TimestampType), false)
			.add("firstPageHit", TimestampType)
			.add("lastPageHit", TimestampType)
			.add("sessionTimeInterval", CalendarIntervalType)
			.add("sessionTimeSeconds", LongType)

		val expectedData = Seq(
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 10:00:00"), Timestamp.valueOf("2015-07-22 11:00:00")),
				Timestamp.valueOf("2015-07-22 10:39:38.707446"),
				Timestamp.valueOf("2015-07-22 10:45:53.245587"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("6 minutes 14.538141 seconds")),
				375.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 11:00:00"), Timestamp.valueOf("2015-07-22 12:00:00")),
				Timestamp.valueOf("2015-07-22 11:00:45.860139"),
				Timestamp.valueOf("2015-07-22 11:55:45.451693"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("54 minutes 59.591554 seconds")),
				3300.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 09:00:00"), Timestamp.valueOf("2015-07-22 10:00:00")),
				Timestamp.valueOf("2015-07-22 09:02:31.935289"),
				Timestamp.valueOf("2015-07-22 09:03:56.860666"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("1 minutes 24.925377 seconds")),
				85.toLong
			)
    	)

		val expectedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(expectedData),
      		expectedSchema
    	)

		val outputDF = AnalyzeJob.runSessionTimeIncludedDF(spark, sessionizedDF)

		assertResult(expectedDF.collect())(AnalyzeJob.runSessionTimeIncludedDF(spark, sessionizedDF).collect())
    }

	test("Analyze Job Test Most Engaged DF") {
		val sessionTimeIncludedDFSchema = new StructType()
			.add("clientAddress",StringType)
			.add("session", new StructType()
				.add("start", TimestampType)
				.add("end", TimestampType), false)
			.add("firstPageHit", TimestampType)
			.add("lastPageHit", TimestampType)
			.add("sessionTimeInterval", CalendarIntervalType)
			.add("sessionTimeSeconds", LongType)

		val sessionTimeIncludedDFData = Seq(
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 10:00:00"), Timestamp.valueOf("2015-07-22 11:00:00")),
				Timestamp.valueOf("2015-07-22 10:39:38.707446"),
				Timestamp.valueOf("2015-07-22 10:45:53.245587"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("6 minutes 14.538141 seconds")),
				375.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 11:00:00"), Timestamp.valueOf("2015-07-22 12:00:00")),
				Timestamp.valueOf("2015-07-22 11:00:45.860139"),
				Timestamp.valueOf("2015-07-22 11:55:45.451693"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("54 minutes 59.591554 seconds")),
				3300.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 09:00:00"), Timestamp.valueOf("2015-07-22 10:00:00")),
				Timestamp.valueOf("2015-07-22 09:02:31.935289"),
				Timestamp.valueOf("2015-07-22 09:03:56.860666"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("1 minutes 24.925377 seconds")),
				85.toLong
			)
    	)

		val sessionTimeIncludedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(sessionTimeIncludedDFData),
      		sessionTimeIncludedDFSchema
    	)

		val expectedSchema = new StructType()
			.add("clientAddress", StringType)
			.add("maxSessionTimeSeconds", LongType)

		val expectedData = Seq(
			Row(
				"180.151.80.140",
				3300.toLong
			)
    	)

		val expectedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(expectedData),
      		expectedSchema
    	)

		assertResult(expectedDF.collect())(AnalyzeJob.runMostEngagedUserDF(spark, sessionTimeIncludedDF).collect())
    }

	test("Analyze Job Test Average Session Time DF") {
		val sessionTimeIncludedDFSchema = new StructType()
			.add("clientAddress",StringType)
			.add("session", new StructType()
				.add("start", TimestampType)
				.add("end", TimestampType), false)
			.add("firstPageHit", TimestampType)
			.add("lastPageHit", TimestampType)
			.add("sessionTimeInterval", CalendarIntervalType)
			.add("sessionTimeSeconds", LongType)

		val sessionTimeIncludedDFData = Seq(
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 10:00:00"), Timestamp.valueOf("2015-07-22 11:00:00")),
				Timestamp.valueOf("2015-07-22 10:39:38.707446"),
				Timestamp.valueOf("2015-07-22 10:45:53.245587"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("6 minutes 14.538141 seconds")),
				375.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 11:00:00"), Timestamp.valueOf("2015-07-22 12:00:00")),
				Timestamp.valueOf("2015-07-22 11:00:45.860139"),
				Timestamp.valueOf("2015-07-22 11:55:45.451693"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("54 minutes 59.591554 seconds")),
				3300.toLong
			),
			Row(
				"180.151.80.140",
				Row(Timestamp.valueOf("2015-07-22 09:00:00"), Timestamp.valueOf("2015-07-22 10:00:00")),
				Timestamp.valueOf("2015-07-22 09:02:31.935289"),
				Timestamp.valueOf("2015-07-22 09:03:56.860666"),
				IntervalUtils.safeStringToInterval(UTF8String.fromString("1 minutes 24.925377 seconds")),
				85.toLong
			)
    	)

		val sessionTimeIncludedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(sessionTimeIncludedDFData),
      		sessionTimeIncludedDFSchema
    	)

		val expectedSchema = new StructType()
			.add("clientAddress", StringType)
			.add("maxSessionTimeSeconds", LongType)

		val expectedData = Seq(
			Row(
				"180.151.80.140",
				1253.toLong
			)
    	)

		val expectedDF = spark.createDataFrame(
      		spark.sparkContext.parallelize(expectedData),
      		expectedSchema
    	)

		assertResult(expectedDF.collect())(AnalyzeJob.runAverageSessionTimeDF(spark, sessionTimeIncludedDF).collect())
    }

	after {
    	spark.stop()
  	}
}
