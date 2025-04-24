package com.databricks.fsi.bpipe

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.util
import scala.collection.JavaConverters._

class RefDataSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "IntradayBar" should "get the right schema" in {
    val options = new CaseInsensitiveStringMap(new util.HashMap[String, String]())
    val ddl = new RefDataSource().inferIntradayBarSchema(options).toDDL
    require(ddl ==
      """SECURITY STRING NOT NULL,TIME TIMESTAMP NOT NULL,OPEN DOUBLE NOT NULL,HIGH DOUBLE NOT NULL,
        |LOW DOUBLE NOT NULL,CLOSE DOUBLE NOT NULL,NUM_EVENTS BIGINT NOT NULL,VOLUME BIGINT NOT NULL,
        |VALUE DOUBLE NOT NULL""".stripMargin.replaceAll("\n", ""))
  }

  "IntradayTick" should "get the right schema" in {
    val options = new CaseInsensitiveStringMap(new util.HashMap[String, String]())
    val ddl = new RefDataSource().inferIntradayTickSchema(options).toDDL
    require(ddl ==
      """SECURITY STRING NOT NULL,TIME TIMESTAMP NOT NULL,TYPE STRING NOT NULL,
        |VALUE DOUBLE NOT NULL,SIZE BIGINT NOT NULL""".stripMargin.replaceAll("\n", ""))
  }

  "ReferenceData" should "get the right schema" in {
    val optionsMap = Map("fields" -> "['BID','TICKER']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    val ddl = new RefDataSource().inferReferenceDataSchema(options).toDDL
    require(ddl ==
      """SECURITY STRING NOT NULL,BID DOUBLE NOT NULL,
        |TICKER STRING NOT NULL""".stripMargin.replaceAll("\n", ""))
  }

  it should "fail if fields are not specified" in {
    val options = new CaseInsensitiveStringMap(new util.HashMap[String, String]())
    assertThrows[IllegalArgumentException] {
      new RefDataSource().inferReferenceDataSchema(options)
    }
  }

  it should "fail if fields are unknown" in {
    val optionsMap = Map("fields" -> "['FOO','BAR']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    assertThrows[IllegalArgumentException] {
      new RefDataSource().inferReferenceDataSchema(options)
    }
  }

  "HistoricalData" should "get the right schema" in {
    val optionsMap = Map("fields" -> "['BID','TICKER']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    val ddl = new RefDataSource().inferHistoricalDataSchema(options).toDDL
    require(ddl ==
      """SECURITY STRING NOT NULL,TIME TIMESTAMP NOT NULL,BID DOUBLE NOT NULL,
        |TICKER STRING NOT NULL""".stripMargin.replaceAll("\n", ""))
  }

  it should "fail if fields are not specified" in {
    val options = new CaseInsensitiveStringMap(new util.HashMap[String, String]())
    assertThrows[IllegalArgumentException] {
      new RefDataSource().inferHistoricalDataSchema(options)
    }
  }

  it should "fail if fields are unknown" in {
    val optionsMap = Map("fields" -> "['FOO','BAR']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    assertThrows[IllegalArgumentException] {
      new RefDataSource().inferHistoricalDataSchema(options)
    }
  }

}
