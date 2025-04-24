package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi.Schema.Datatype
import com.bloomberglp.blpapi.{Datetime, Element, Name}
import com.databricks.fsi.bpipe.BPipeUtils._
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class BPipeUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  trait GenericElement extends Element {
    override def prettyPrint(i: Int): lang.StringBuilder = new java.lang.StringBuilder()

    override def name(): Name = new Name("FOO")

    override def numValues(): Int = 0

    override def numElements(): Int = 0
  }

  case class TestIntElement(value: Int) extends GenericElement {
    override def datatype(): Datatype = Datatype.INT32

    override def getValueAsInt32: Int = value
  }

  case class TestLongElement(value: Long) extends GenericElement {
    override def datatype(): Datatype = Datatype.INT64

    override def getValueAsInt64: Long = value
  }

  case class TestFloatElement(value: Float) extends GenericElement {
    override def datatype(): Datatype = Datatype.FLOAT32

    override def getValueAsFloat32: Float = value
  }

  case class TestDoubleElement(value: Double) extends GenericElement {
    override def datatype(): Datatype = Datatype.FLOAT64

    override def getValueAsFloat64: Double = value
  }

  case class TestStringElement(value: String) extends GenericElement {
    override def datatype(): Datatype = Datatype.STRING

    override def getElementAsString(name: String): String = value

    override def getElementAsString(name: Name): String = value

    override def getValueAsString: String = value
  }

  case class TestEnumElement(value: String) extends GenericElement {
    override def datatype(): Datatype = Datatype.ENUMERATION

    override def getValueAsString: String = value

  }

  case class TestBooleanElement(value: Boolean) extends GenericElement {
    override def datatype(): Datatype = Datatype.BOOL

    override def getValueAsBool: Boolean = value
  }

  case class TestDatetimeElement(value: Datetime) extends GenericElement {
    override def datatype(): Datatype = Datatype.DATETIME

    override def getValueAsDatetime: Datetime = value
  }

  case class TestListElement(values: List[TestStringElement]) extends GenericElement {
    override def datatype(): Datatype = Datatype.SEQUENCE

    override def numValues(): Int = values.size

    override def getValueAsElement(index: Int): Element = values(index)

  }

  "writer" should "support generic types" in {

    val writer = new UnsafeRowWriter(7)
    writer.resetRowWriter()

    writer.writeElement(0, TestIntElement(1))
    writer.writeElement(1, TestLongElement(1L))
    writer.writeElement(2, TestFloatElement(1.0f))
    writer.writeElement(3, TestDoubleElement(1.0))
    writer.writeElement(4, TestStringElement("FOO"))
    writer.writeElement(5, TestBooleanElement(true))
    writer.writeElement(6, TestEnumElement("BAR"))

    writer.getRow.getInt(0) should be(1)
    writer.getRow.getLong(1) should be(1L)
    writer.getRow.getFloat(2) should be(1.0f)
    writer.getRow.getDouble(3) should be(1.0)
    writer.getRow.getString(4) should be("FOO")
    writer.getRow.getBoolean(5) shouldBe true
    writer.getRow.getString(6) should be("BAR")

  }

  it should "support date time objects" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val locale = ZoneId.systemDefault()
    val writer = new UnsafeRowWriter(1)
    writer.resetRowWriter()
    writer.writeElement(0, TestDatetimeElement(new Datetime(2023, 1, 1, 12, 32, 22, 123)), locale)
    DateTimeUtils.toJavaTimestamp(writer.getRow.get(0, TimestampType).asInstanceOf[Long]) should be(new Timestamp(sdf.parse("2023-01-01 12:32:22.123").getTime))
  }

  "writer" should "support lists of string" in {
    val element = TestListElement(List(TestStringElement("foo"), TestStringElement("bar")))
    val writer = new UnsafeRowWriter(1)
    writer.resetRowWriter()
    writer.writeElement(0, element)
    val a = writer.getRow.get(0, ArrayType(StringType)).asInstanceOf[UnsafeArrayData]
    val actual = (0 until a.numElements()).map(i => a.getUTF8String(i).toString)
    actual should be(List("foo", "bar"))
  }

  "Date" should "support different timezones" in {
    val dt = new Datetime(2023, 1, 1, 12, 32, 22, 123)
    dt.convert(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) should be("2023-01-01T12:32:22.123")
  }

}
