package com.databricks.fsi.bpipe

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.util.Date

@BPipeEnvironmentTest
class RefDataSparkIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val aMonthAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusMonths(1).toInstant))
  val aYearAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusYears(1).toInstant))
  val now: String = sdf.format(Date.from(ZonedDateTime.now().toInstant))
  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    // Explicitly configure log4j to use our properties file
    System.setProperty("log4j.configuration", "log4j.properties")

    // Force log4j to reconfigure
    import org.apache.log4j.LogManager
    LogManager.resetConfiguration()
    LogManager.getRootLogger()

    spark = SparkSession.builder()
      .appName(BLP_REFDATA)
      .master("local[2]")
      .getOrCreate()

    // Set Spark context log level to ERROR to minimize noise
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

  "ReferenceDataRequest" should "fully processed" in {
    spark
      .read
      .format("//blp/refdata")

      // B-PIPE connection
      .option("serviceHost", "127.0.0.1")
      .option("servicePort", 8954)
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "ReferenceDataRequest")
      .option("fields", "['PX_LAST','BID','ASK','TICKER','CHAIN_TICKERS']")
      .option("securities", "['SPY US EQUITY','MSFT US EQUITY','AAPL 150117C00600000 EQUITY']")
      .option("overrides", "{'CHAIN_PUT_CALL_TYPE_OVRD':'C','CHAIN_POINTS_OVRD':'3','CHAIN_EXP_DT_OVRD':'20141220'}")

      // Custom logic
      .option("partitions", "[0,1,1]")
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

  "HistoricalDataRequest" should "fully processed" in {
    spark
      .read
      .format("//blp/refdata")

      // B-PIPE connection
      .option("serviceHost", "127.0.0.1")
      .option("servicePort", 8954)
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "HistoricalDataRequest")
      .option("fields", "['BID','ASK']")
      .option("securities", "['SPY US EQUITY','MSFT US EQUITY','AAPL 150117C00600000 EQUITY']")
      .option("startDate", aYearAgo)
      .option("endDate", now)
      .option("periodicityAdjustment", "ACTUAL")
      .option("periodicitySelection", "WEEKLY")
      .option("pricingOption", "PRICING_OPTION_PRICE")
      .option("adjustmentNormal", value = false)
      .option("adjustmentAbnormal", value = false)
      .option("adjustmentSplit", value = false)
      .option("maxDataPoints", 10)
      .option("overrideOption", "OVERRIDE_OPTION_GPA")

      // Custom logic
      .option("partitions", "[0,1,1]")
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

  "IntradayTickRequest" should "fully processed" in {
    spark
      .read
      .format("//blp/refdata")

      // B-PIPE connection
      .option("serviceHost", "127.0.0.1")
      .option("servicePort", 8954)
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "IntradayTickRequest")
      .option("security", "SPY US EQUITY")
      .option("startDateTime", aMonthAgo)
      .option("endDateTime", now)
      .option("eventTypes", "['BID', 'ASK']")
      .option("returnEids", value = false)
      .option("includeConditionCodes", value = false)
      .option("includeBicMicCodes", value = false)
      .option("includeBrokerCodes", value = false)
      .option("includeRpsCodes", value = false)
      .option("includeExchangeCodes", value = false)
      .option("includeNonPlottableEvents", value = false)

      // Custom logic
      .option("partitions", 5)
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

  "IntradayTickBarRequest" should "fully processed" in {
    spark
      .read
      .format("//blp/refdata")

      // B-PIPE connection
      .option("serviceHost", "127.0.0.1")
      .option("servicePort", 8954)
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "IntradayBarRequest")
      .option("interval", 60)
      .option("security", "SPY US EQUITY")
      .option("startDateTime", aMonthAgo)
      .option("endDateTime", now)
      .option("eventType", "BID")
      .option("returnEids", value = false)
      .option("gapFillInitialBar", value = false)
      .option("adjustmentNormal", value = false)
      .option("adjustmentAbnormal", value = false)
      .option("adjustmentSplit", value = false)
      .option("adjustmentFollowDPDF", value = false)

      // Custom logic
      .option("partitions", 5)
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

}
