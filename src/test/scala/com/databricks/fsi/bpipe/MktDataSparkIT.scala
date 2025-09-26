package com.databricks.fsi.bpipe

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

@BPipeEnvironmentTest
class MktDataSparkIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName(BLP_MKTDATA)
      .master("local[1]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

  "Real time feed" should "fully processed" in {

    spark
      .readStream
      .format("//blp/mktdata")

      // B-PIPE connection
      .option("serverAddresses", "['SERVER1', 'SERVER2']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "/path/to/rootCertificate.pk7")
      .option("tlsPrivateKeyPath", "/path/to/privateKey.pk12")
      .option("tlsPrivateKeyPassword", "password")
      .option("authApplicationName", "APP_NAME")
      .option("correlationId", 999)

      // Service configuration
      .option("fields", "['MKTDATA_EVENT_TYPE','MKTDATA_EVENT_SUBTYPE','EID','BID','ASK','IS_DELAYED_STREAM','TRADE_UPDATE_STAMP_RT']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")

      // Custom logic
      .option("timezone", "America/New_York")
      .option("permissive", value = false)

      // Start stream ingest
      .load
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination(20000)
  }

}
