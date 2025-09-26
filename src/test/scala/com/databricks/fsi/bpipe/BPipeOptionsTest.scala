package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.BPipeConfig.BpipeApiConfig
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

class BPipeOptionsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val tempCertFile = "/dummy_crt.pk7"
  private val tempKeyFile = "/dummy_key.pk12"

  "Options" should "be parsed" in {

    var options = new CaseInsensitiveStringMap(Map("list" -> "['BID', 'ASK']").asJava)
    options.getStringList("list") should be(List("BID", "ASK"))
    options.getStringListOpt("list") should be(List("BID", "ASK"))
    options.getStringListOpt("FOO") should be(List.empty[String])
    assertThrows[IllegalArgumentException] {
      options = new CaseInsensitiveStringMap(Map("list" -> "FOO").asJava)
      options.getStringList("list")
    }

    options = new CaseInsensitiveStringMap(Map("string" -> "antoine").asJava)
    options.getString("string") should be("antoine")
    options.getStringOpt("string") should be(Some("antoine"))
    options.getStringOpt("foo") should be(None)

    options = new CaseInsensitiveStringMap(Map("integer" -> "1").asJava)
    options.getInt("integer") should be(1)
    options.getIntOpt("integer") should be(Some(1))
    options.getIntOpt("foo") should be(None)

    options = new CaseInsensitiveStringMap(Map("double" -> "1.0").asJava)
    options.getDouble("double") should be(1.0)
    options.getDoubleOpt("double") should be(Some(1.0))
    options.getDoubleOpt("foo") should be(None)

    options = new CaseInsensitiveStringMap(Map("bool" -> "true").asJava)
    options.getBoolean("bool") should be(true)
    options.getBooleanOpt("bool") should be(Some(true))
    options.getBooleanOpt("foo") should be(None)

    options = new CaseInsensitiveStringMap(Map("long" -> "1").asJava)
    options.getLong("long") should be(1L)
    options.getLongOpt("long") should be(Some(1L))
    options.getLongOpt("foo") should be(None)

    options = new CaseInsensitiveStringMap(Map("map" -> "{'foo':'bar'}").asJava)
    options.getStringMap("map") should be(Map("foo" -> "bar"))
    options.getStringMapOpt("map") should be(Map("foo" -> "bar"))
    options.getStringMapOpt("foo") should be(Map.empty[String, String])
    assertThrows[IllegalArgumentException] {
      options = new CaseInsensitiveStringMap(Map("map" -> "FOO").asJava)
      options.getStringMap("map")
    }

    val dateSDF = new SimpleDateFormat("yyyy-MM-dd")
    options = new CaseInsensitiveStringMap(Map("date" -> "2023-01-01").asJava)
    options.getDate("date") should be(dateSDF.parse("2023-01-01"))
    options.getDateOpt("date") should be(Some(dateSDF.parse("2023-01-01")))
    options.getDateOpt("foo") should be(None)
    assertThrows[IllegalArgumentException] {
      options = new CaseInsensitiveStringMap(Map("date" -> "foo").asJava)
      options.getDate("date")
    }

    val timeSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    options = new CaseInsensitiveStringMap(Map("timestamp" -> "2023-01-01 12:34:21.000").asJava)
    options.getTimestamp("timestamp") should be(timeSDF.parse("2023-01-01 12:34:21.000"))
    options.getTimestampOpt("timestamp") should be(Some(timeSDF.parse("2023-01-01 12:34:21.000")))
    options.getTimestampOpt("foo") should be(None)
    assertThrows[IllegalArgumentException] {
      options = new CaseInsensitiveStringMap(Map("timestamp" -> "foo").asJava)
      options.getTimestamp("timestamp")
    }

    options = new CaseInsensitiveStringMap(Map("list" -> "[1, 2]").asJava)
    options.getIntList("list") should be(List(1, 2))
    options.getIntListOpt("list") should be(List(1, 2))
    options.getIntListOpt("FOO") should be(List.empty[Int])
    assertThrows[IllegalArgumentException] {
      options = new CaseInsensitiveStringMap(Map("list" -> "FOO").asJava)
      options.getIntList("list")
    }
  }

  "MktDataApiConfig" should "be created with valid options and existing certificate files" in {
    val options = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['127.0.0.1','127.0.0.2']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> tempCertFile.toString,
      "tlsPrivateKeyPath" -> tempKeyFile.toString,
      "tlsPrivateKeyPassword" -> "testPassword123",
      "authApplicationName" -> "blp:test-app",
      "correlationId" -> "777"
    ).asJava)

    val config = BpipeApiConfig(options)

    config.serverAddresses should be(Array("127.0.0.1", "127.0.0.2"))
    config.serverPort should be(8194)
    config.tlsCertificatePath should be(tempCertFile.toString)
    config.tlsPrivateKeyPath should be(tempKeyFile.toString)
    config.tlsPrivateKeyPassword should be("testPassword123")
    config.authApplicationName should be("blp:test-app")
    config.correlationId should be(777L)
  }

  it should "fail when certificate file does not exist" in {
    val options = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['127.0.0.1']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> "/nonexistent/path/cert.p12",
      "tlsPrivateKeyPath" -> tempKeyFile.toString,
      "tlsPrivateKeyPassword" -> "testPassword123",
      "authApplicationName" -> "blp:test-app",
      "correlationId" -> "777"
    ).asJava)

    assertThrows[IllegalArgumentException] {
      BpipeApiConfig(options)
    }
  }

  it should "fail when private key file does not exist" in {
    val options = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['gbr.cloudpoint.bloomberg.com']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> tempCertFile.toString,
      "tlsPrivateKeyPath" -> "/nonexistent/path/key.p12",
      "tlsPrivateKeyPassword" -> "testPassword123",
      "authApplicationName" -> "blp:test-app",
      "correlationId" -> "777"
    ).asJava)

    assertThrows[IllegalArgumentException] {
      BpipeApiConfig(options)
    }
  }

  it should "fail with missing required TLS options" in {
    val incompleteOptions = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['gbr.cloudpoint.bloomberg.com']",
      "serverPort" -> "8194"
      // Missing TLS certificate paths and other required fields
    ).asJava)
    assertThrows[Exception] {
      BpipeApiConfig(incompleteOptions)
    }
  }

  it should "handle single server address" in {
    val options = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['single.server.com']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> tempCertFile.toString,
      "tlsPrivateKeyPath" -> tempKeyFile.toString,
      "tlsPrivateKeyPassword" -> "password",
      "authApplicationName" -> "blp:test",
      "correlationId" -> "123"
    ).asJava)
    val config = BpipeApiConfig(options)
    config.serverAddresses should be(Array("single.server.com"))
    config.serverPort should be(8194)
  }

  it should "handle multiple server addresses" in {
    val options = new CaseInsensitiveStringMap(Map(
      "serverAddresses" -> "['server1.com','server2.com','server3.com']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> tempCertFile.toString,
      "tlsPrivateKeyPath" -> tempKeyFile.toString,
      "tlsPrivateKeyPassword" -> "password",
      "authApplicationName" -> "blp:test",
      "correlationId" -> "456"
    ).asJava)

    val config = BpipeApiConfig(options)

    config.serverAddresses should be(Array("server1.com", "server2.com", "server3.com"))
  }

  it should "validate all required fields are present" in {
    val validOptions = Map(
      "serverAddresses" -> "['test.com']",
      "serverPort" -> "8194",
      "tlsCertificatePath" -> tempCertFile.toString,
      "tlsPrivateKeyPath" -> tempKeyFile.toString,
      "tlsPrivateKeyPassword" -> "password",
      "authApplicationName" -> "blp:test",
      "correlationId" -> "123"
    )

    // Test each required field by removing it
    validOptions.keys.foreach { keyToRemove =>
      val incompleteOptions = new CaseInsensitiveStringMap(
        (validOptions - keyToRemove).asJava
      )

      assertThrows[Exception] {
        BpipeApiConfig(incompleteOptions)
      }
    }
  }


}
