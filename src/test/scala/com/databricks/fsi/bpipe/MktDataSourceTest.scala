package com.databricks.fsi.bpipe

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.util
import scala.collection.JavaConverters._

class MktDataSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "MarketData" should "get the right schema" in {
    val optionsMap = Map("fields" -> "['BID','TICKER']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    val ddl = new MktDataSource().inferSchema(options).toDDL
    require(ddl ==
      """SECURITY STRING,BID DOUBLE,
        |TICKER STRING""".stripMargin.replaceAll("\n", ""))
  }

  it should "fail if fields are not specified" in {
    val options = new CaseInsensitiveStringMap(new util.HashMap[String, String]())
    assertThrows[IllegalArgumentException] {
      new MktDataSource().inferSchema(options)
    }
  }

  it should "fail if fields are unknown" in {
    val optionsMap = Map("fields" -> "['FOO','BAR']")
    val options = new CaseInsensitiveStringMap(optionsMap.asJava)
    assertThrows[IllegalArgumentException] {
      new MktDataSource().inferSchema(options)
    }
  }
}
