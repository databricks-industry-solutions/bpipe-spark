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
      .option("serverAddresses", "['gbr.cloudpoint.bloomberg.com', 'deu.cloudpoint.bloomberg.com']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/rootCertificate.pk7")
      .option("tlsPrivateKeyPath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/073BE6888AE987A5FC5C3C288CBC89E3.pk12")
      .option("tlsPrivateKeyPassword", "VcRC3uY48vp2wZj5")
      .option("authApplicationName", "blp:dbx-src-test")
      .option("correlationId", 999)

      // Service configuration
      .option("fields", "['MKTDATA_EVENT_TYPE','MKTDATA_EVENT_SUBTYPE','EID','BID','ASK','IS_DELAYED_STREAM','LAST_UPDATE_ASK_RT','LAST_UPDATE_BID_RT','TRADE_UPDATE_STAMP_RT']")
      .option("securities", "['0UU5C 95.3750 COMB Comdty']")
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
