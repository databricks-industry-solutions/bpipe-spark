package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.BPipeConfig._
import com.google.gson.Gson
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import scala.collection.JavaConverters._

class BPipeConfigTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "dates" should "use only one partition" in {
    val toDate = new Date()
    val fromDate = new Date(toDate.getTime - 24 * 3600 * 1000)
    val optionsMap = new util.HashMap[String, String]()
    val options = new CaseInsensitiveStringMap(optionsMap)
    val partitions = BPipeConfig.partitionByDate(options, fromDate, toDate)
    partitions.length should be(1)
  }

  it should "support multiple partitions" in {
    val toDate = new Date()
    val fromDate = new Date(toDate.getTime - 24 * 3600 * 1000)
    val optionsMap = Map("partitions" -> 10.toString).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val partitions = BPipeConfig.partitionByDate(options, fromDate, toDate)
    partitions.length should be(10)
    partitions.map(t => t._2.getTime - t._1.getTime).exists(d => d != 8640000) should not be true
  }

  "securities" should "use only one partition" in {
    val securities = (0 until 100).map(_ => UUID.randomUUID().toString).toList
    val optionsMap = new util.HashMap[String, String]()
    val options = new CaseInsensitiveStringMap(optionsMap)
    val partitions = BPipeConfig.partitionBySecurities(options, securities)
    partitions.length should be(1)
  }

  it should "be partitioned randomly" in {
    val securities = (0 until 100).map(_ => UUID.randomUUID().toString).toList
    val expectedPartitions = 3
    val optionsMap = Map("partitions" -> expectedPartitions.toString).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val partitions = BPipeConfig.partitionBySecurities(options, securities)
    partitions.length should be(expectedPartitions)
  }

  it should "support explicit partitioning" in {
    val expectedPartitions = 3
    val securityMapping = (0 until expectedPartitions).flatMap(partition => {
      (0 until 3).map(_ => UUID.randomUUID().toString).toList.map(security => {
        (security, partition)
      })
    }).toMap
    val optionsMap = Map("partitions" -> new Gson().toJson(securityMapping.values.asJava)).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val partitions = BPipeConfig.partitionBySecurities(options, securityMapping.keys.toList)
    partitions.length should be(expectedPartitions)
    partitions.exists(_.size != 3) should not be true
  }

  "ApiConfig" should "be constructed from options" in {
    val optionsMap = Map(
      "serviceHost" -> "127.0.0.1",
      "servicePort" -> "85295",
      "correlationId" -> "999"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    RefDataApiConfig(options) should be(RefDataApiConfig(
      "127.0.0.1",
      85295,
      999
    ))
  }

  it should "fail if option is missing" in {
    val optionsMap = Map(
      "servicePort" -> "85295",
      "correlationId" -> "999"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    assertThrows[IllegalArgumentException] {
      RefDataApiConfig(options)
    }
  }

  it should "fail if option is invalid" in {
    val optionsMap = Map(
      "serviceHost" -> "127.0.0.1",
      "servicePort" -> "85295",
      "correlationId" -> "INVALID_LONG"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val caught = intercept[IllegalArgumentException] {
      RefDataApiConfig(options)
    }
    caught.getMessage.split("\\s").head should be("[correlationId]")
  }

  "MktDataConfig" should "be constructed from options" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    MktDataConfig(options) should be(MktDataConfig(
      List("AAPL", "MSFT"),
      List("BID", "ASK")
    ))
  }

  it should "fail is option is missing" in {
    val optionsMap = Map(
      "securities" -> "['AAPL', 'MSFT']"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    assertThrows[IllegalArgumentException] {
      MktDataConfig(options)
    }
  }

  it should "fail is option is invalid" in {
    val optionsMap = Map(
      "fields" -> "INVALID",
      "securities" -> "['AAPL', 'MSFT']"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val caught = intercept[IllegalArgumentException] {
      MktDataConfig(options)
    }
    caught.getMessage.split("\\s").head should be("[fields]")
  }

  it should "support partitioning" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "partitions" -> "[0,1]"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    MktDataConfig(options).buildPartitions(options).map(i => {
      i.asInstanceOf[MktDataConfig].securities
    }) should be(List(
      List("AAPL"),
      List("MSFT")
    ))
  }

  "RefDataConfig" should "be constructed from options" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "overrides" -> "{'foo': 'bar'}"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataConfig(options)
    config should be(RefDataConfig(
      List("AAPL", "MSFT"),
      List("BID", "ASK"),
      Map("foo" -> "bar")
    ))
    config.validate()
  }

  it should "fail is option is missing" in {
    val optionsMap = Map(
      "securities" -> "['AAPL', 'MSFT']",
      "overrides" -> "{'foo': 'bar'}"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    assertThrows[IllegalArgumentException] {
      RefDataConfig(options)
    }
  }

  it should "fail is option is invalid" in {
    val optionsMap = Map(
      "fields" -> "INVALID",
      "securities" -> "['AAPL', 'MSFT']",
      "overrides" -> "{'foo': 'bar'}"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val caught = intercept[IllegalArgumentException] {
      RefDataConfig(options)
    }
    caught.getMessage.split("\\s").head should be("[fields]")
  }

  it should "fail is override is invalid" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "overrides" -> "INVALID"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val caught = intercept[IllegalArgumentException] {
      RefDataConfig(options)
    }
    caught.getMessage.split("\\s").head should be("[overrides]")
  }

  it should "support partitioning" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "overrides" -> "{'foo': 'bar'}",
      "partitions" -> "[0,1]"
    ).asJava
    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataConfig(options)
    config.buildPartitions(options).map(i => {
      i.asInstanceOf[RefDataConfig].securities
    }) should be(List(
      List("AAPL"),
      List("MSFT")
    ))
  }

  "RefDataHistoricalConfig" should "be constructed from options" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "startDate" -> "2022-01-01",
      "endDate" -> "2022-02-01",
      "periodicityAdjustment" -> "ACTUAL",
      "periodicitySelection" -> "WEEKLY",
      "pricingOption" -> "PRICING_OPTION_PRICE",
      "adjustmentNormal" -> "false",
      "adjustmentAbnormal" -> "false",
      "adjustmentSplit" -> "false",
      "maxDataPoints" -> "5",
      "overrideOption" -> "OVERRIDE_OPTION_GPA"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataHistoricalConfig(options)
    config should be(RefDataHistoricalConfig(
      List("AAPL", "MSFT"),
      List("BID", "ASK"),
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      Some("ACTUAL"),
      Some("WEEKLY"),
      Some("PRICING_OPTION_PRICE"),
      Some(false),
      Some(false),
      Some(false),
      Some(5),
      Some("OVERRIDE_OPTION_GPA")
    ))

    config.validate()
    assertThrows[IllegalArgumentException] {
      config.copy(periodicityAdjustment = Some("FOO")).validate()
    }
  }

  it should "ignore non defined properties" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "startDate" -> "2022-01-01",
      "endDate" -> "2022-02-01"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataHistoricalConfig(options)
    config should be(RefDataHistoricalConfig(
      List("AAPL", "MSFT"),
      List("BID", "ASK"),
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ))

    config.validate()
  }

  it should "support partitioning" in {
    val optionsMap = Map(
      "fields" -> "['BID', 'ASK']",
      "securities" -> "['AAPL', 'MSFT']",
      "startDate" -> "2022-01-01",
      "endDate" -> "2022-02-01",
      "partitions" -> "[0,1]"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataHistoricalConfig(options)
    config.buildPartitions(options).map(i => {
      i.asInstanceOf[RefDataHistoricalConfig].securities
    }) should be(List(
      List("AAPL"),
      List("MSFT")
    ))
  }

  "RefDataTickDataConfig" should "be constructed from options" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "security" -> "AAPL",
      "eventTypes" -> "['BID', 'ASK']",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01",
      "returnEids" -> "true",
      "includeConditionCodes" -> "true",
      "includeExchangeCodes" -> "true",
      "includeNonPlottableEvents" -> "true",
      "includeBrokerCodes" -> "true",
      "includeRpsCodes" -> "true",
      "includeBicMicCodes" -> "true"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataTickDataConfig(options)
    config should be(RefDataTickDataConfig(
      "AAPL",
      List("BID", "ASK"),
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true)
    ))

    config.validate()
    assertThrows[IllegalArgumentException] {
      config.copy(eventTypes = List("FOO")).validate()
    }
  }

  it should "ignore non defined properties" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "security" -> "AAPL",
      "eventTypes" -> "['BID', 'ASK']",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataTickDataConfig(options)
    config should be(RefDataTickDataConfig(
      "AAPL",
      List("BID", "ASK"),
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ))

    config.validate()
  }

  it should "support partitioning" in {
    val optionsMap = Map(
      "security" -> "AAPL",
      "eventTypes" -> "['BID', 'ASK']",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01",
      "partitions" -> "3"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataTickDataConfig(options)
    config.buildPartitions(options).map(i => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val c = i.asInstanceOf[RefDataTickDataConfig]
      (
        sdf.format(c.startDateTime),
        sdf.format(c.endDateTime)
      )
    }) should be(List(
      ("2022-01-01", "2022-01-11"),
      ("2022-01-11", "2022-01-21"),
      ("2022-01-21", "2022-02-01")
    ))
  }

  "RefDataBarDataConfig" should "be constructed from options" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "security" -> "AAPL",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01",
      "interval" -> "100",
      "eventType" -> "TRADE",
      "returnEids" -> "true",
      "gapFillInitialBar" -> "true",
      "adjustmentNormal" -> "true",
      "adjustmentAbnormal" -> "true",
      "adjustmentSplit" -> "true",
      "adjustmentFollowDPDF" -> "true"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataBarDataConfig(options)
    config should be(RefDataBarDataConfig(
      "AAPL",
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      100,
      Some("TRADE"),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true),
      Some(true)
    ))

    config.validate()
    assertThrows[IllegalArgumentException] {
      config.copy(eventType = Some("FOO")).validate()
    }
    assertThrows[IllegalArgumentException] {
      config.copy(interval = 0).validate()
    }
  }

  it should "ignore non defined properties" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val optionsMap = Map(
      "security" -> "AAPL",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01",
      "interval" -> "100"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataBarDataConfig(options)
    config should be(RefDataBarDataConfig(
      "AAPL",
      sdf.parse("2022-01-01"),
      sdf.parse("2022-02-01"),
      100,
      None,
      None,
      None,
      None,
      None,
      None
    ))

    config.validate()
  }

  it should "support partitioning" in {
    val optionsMap = Map(
      "security" -> "AAPL",
      "startDateTime" -> "2022-01-01",
      "endDateTime" -> "2022-02-01",
      "interval" -> "100",
      "partitions" -> "3"
    ).asJava

    val options = new CaseInsensitiveStringMap(optionsMap)
    val config = RefDataBarDataConfig(options)
    config.buildPartitions(options).map(i => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val c = i.asInstanceOf[RefDataBarDataConfig]
      (
        sdf.format(c.startDateTime),
        sdf.format(c.endDateTime)
      )
    }) should be(List(
      ("2022-01-01", "2022-01-11"),
      ("2022-01-11", "2022-01-21"),
      ("2022-01-21", "2022-02-01")
    ))
  }
}
