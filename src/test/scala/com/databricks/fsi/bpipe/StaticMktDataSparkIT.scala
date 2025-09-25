package com.databricks.fsi.bpipe

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.util.Date

@BPipeEnvironmentTest
class StaticMktDataSparkIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val aMonthAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusMonths(1).toInstant))
  val aYearAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusYears(1).toInstant))
  val now: String = sdf.format(Date.from(ZonedDateTime.now().toInstant))
  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName(BLP_STATICMKTDATA)
      .master("local[2]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

  "ReferenceDataRequest" should "fully processed" in {
    spark
      .read
      .format("//blp/staticMktData")

      // B-PIPE connection
      .option("serverAddresses", "['gbr.cloudpoint.bloomberg.com', 'deu.cloudpoint.bloomberg.com']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/rootCertificate.pk7")
      .option("tlsPrivateKeyPath", "/Users/antoine.amend/Workspace/bloomberg/bpipe-spark/credentials/073BE6888AE987A5FC5C3C288CBC89E3.pk12")
      .option("tlsPrivateKeyPassword", "VcRC3uY48vp2wZj5")
      .option("authApplicationName", "blp:dbx-src-test")
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "ReferenceDataRequest")
      .option("fields", "['BID', 'ASK', 'LAST_PRICE']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")
      .option("returnEids", true)

      // Custom logic
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

}