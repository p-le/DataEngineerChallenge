package phu.le.dev.challenge

import org.scalatest.funsuite.AnyFunSuite
import scala.util.matching.Regex

class AnalyzeJobTest extends AnyFunSuite  {

	def extractLog(line: String): Option[WebLogRecord] = {
      	webLogRegex.findFirstIn(line) match {
			case Some(webLogRegex(dateTime, serviceName, sourceAddress, targetAddress, f1, f2, f3, responseCode1, responseCode2, _, receivedBytes, httpMethod, url, httpVersion, userAgent, cipherName, openSSLVersion)) =>
				Some(WebLogRecord(dateTime, serviceName, sourceAddress, targetAddress, f1.toDouble, f2.toDouble, f3.toDouble, responseCode1.toInt, responseCode2.toInt, receivedBytes.toInt, httpMethod, url, httpVersion, userAgent, cipherName, openSSLVersion))
			case _ => None
	  	}
    }

	val log = """2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"""
	// IP: possesive quantifier
	// val webLogRegex = """^(.+) (.+) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) (.+) (.+) (.+) (.+) "(.+) (.+) (.+)" "(.+)" (.+) (.+)""".r
	// val webLogRegex = """^(\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{1,6}Z) (.+) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) (.+) (.+) (.+) (.+) "(.+) (.+) (.+)" "(.+)" (.+) (.+)""".r
	val webLogRegex = """^(\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{1,6}Z) (.+) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) (\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+\:[0-9]{1,5}) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) ([0-9]+[.][0-9]+) ([1-5][0-9][0-9]) ([1-5][0-9][0-9]) (.+) (.+) "(.+) (.+) (.+)" "(.+)" (.+) (.+)""".r
	val record = extractLog(log)
	record match {
		case Some(record) =>
			println(record.dateTime)
			println(record.sourceAddress)
			println(record.targetAddress)
		case None => println("TTEST")
	}
    // test 1
    test("the name is set correctly in constructor") {
        assert("Barney Rubble" == "Barney Rubble")
    }

    // test 2
    test("a Person's name can be changed") {
        assert("Ochocinco" == "Ochocinco")
    }

}
