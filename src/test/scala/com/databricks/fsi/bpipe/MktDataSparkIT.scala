package com.databricks.fsi.bpipe

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class MktDataSparkIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().appName(BLP_REFDATA).master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

  "Real time feed" should "fully processed" in {

    spark
      .readStream
      .format("//blp/mktdata")

      // B-PIPE connection
      .option("serviceHost", "127.0.0.1")
      .option("servicePort", 8954)
      .option("correlationId", 999)

      // Service configuration
      .option("fields", "['BID','ASK','TRADE_UPDATE_STAMP_RT']")
      .option("securities", "['SPY US EQUITY','MSFT US EQUITY']")

      // Custom logic
      .option("partitions", "[1,2]")
      .option("timezone", "America/New_York")
      .option("permissive", value = true)

      // Start stream ingest
      .load
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination(20000)
  }

}
