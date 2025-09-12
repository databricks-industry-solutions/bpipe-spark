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

  var spark: SparkSession = _
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val aMonthAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusMonths(1).toInstant))
  val aYearAgo: String = sdf.format(Date.from(ZonedDateTime.now().minusYears(1).toInstant))
  val now: String = sdf.format(Date.from(ZonedDateTime.now().toInstant))

  override protected def beforeAll(): Unit = {
    // Explicitly configure log4j to use our properties file
    System.setProperty("log4j.configuration", "log4j.properties")
    
    // Force log4j to reconfigure
    import org.apache.log4j.LogManager
    LogManager.resetConfiguration()
    LogManager.getRootLogger()
    
    spark = SparkSession.builder()
      .appName(BLP_STATICMKTDATA)
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
      .format("//blp/staticMktData")

      // B-PIPE connection
      .option("serverAddresses", "['IP_ADDRESS_1', 'IP_ADDRESS_2']")
      .option("serverPort", 8194)
      .option("tlsCertificatePath", "PATH_TO_CERTIFICATE.pk7")
      .option("tlsPrivateKeyPath", "PATH_TO_PRIVATE_KEY.pk12")
      .option("tlsPrivateKeyPassword", "PRIVATE_KEY_PASSWORD")
      .option("authApplicationName", "APP_NAME")
      .option("correlationId", 999)

      // Service configuration
      .option("serviceName", "ReferenceDataRequest")
      .option("fields", "['BID', 'ASK', 'LAST_PRICE']")
      .option("securities", "['BBHBEAT Index', 'GBP BGN Curncy', 'EUR BGN Curncy', 'JPYEUR BGN Curncy']")

      // Custom logic
      .option("timezone", "America/New_York")

      // Start batch ingest
      .load
      .show(20, truncate = false)
  }

}