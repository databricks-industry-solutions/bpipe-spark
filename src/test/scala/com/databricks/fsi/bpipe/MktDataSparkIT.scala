package com.databricks.fsi.bpipe

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

@BPipeEnvironmentTest
class MktDataSparkIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {

    // Explicitly configure log4j to use our properties file
    System.setProperty("log4j.configuration", "log4j.properties")

    // Force log4j to reconfigure
    import org.apache.log4j.LogManager
    LogManager.resetConfiguration()
    LogManager.getRootLogger()

    spark = SparkSession.builder()
      .appName(BLP_MKTDATA)
      .master("local[1]")
      .getOrCreate()

    // Set Spark context log level to ERROR to minimize noise
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

  "Real time feed" should "fully processed" in {

    spark
      .readStream
      .format("//blp/mktdata")

      // B-PIPE connection
      .option("serverAddresses", "['gbr.cloudpoint.bloomberg.com', 'deu.cloudpoint.bloomberg.com']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/rootCertificate.pk7")
      .option("tlsPrivateKeyPath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/073BE6888AE987A5FC5C3C288CBC89E3.pk12")
      .option("tlsPrivateKeyPassword", "VcRC3uY48vp2wZj5")
      .option("authApplicationName", "blp:dbx-src-test")
      .option("correlationId", 999)

      // Service configuration
      .option("fields", "['MKTDATA_EVENT_TYPE','MKTDATA_EVENT_SUBTYPE','EID','BID','ASK','IS_DELAYED_STREAM','TRADE_UPDATE_STAMP_RT']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")

      // Custom logic
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
