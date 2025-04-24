package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi.Schema.Datatype._
import com.bloomberglp.blpapi.{Datetime, Element}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import scala.annotation.tailrec

object BPipeUtils {

  implicit class DatetimeImpl(dt: Datetime) {
    def write(writer: UnsafeRowWriter, i: Int, zoneId: ZoneId): Unit = {
      val zdt = dt.convert(zoneId)
      writer.write(i, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(zdt.toLocalDateTime)))
    }

    def convert(zoneId: ZoneId): ZonedDateTime = {
      ZonedDateTime.ofInstant(dt.calendar().toInstant, zoneId)
    }
  }


  implicit class WriterImpl(writer: UnsafeRowWriter) {

    def writeElement(offset: Int, fieldData: Element, zoneId: ZoneId = ZoneId.of("UTC")): Unit = {
      fieldData.datatype() match {
        case INT32 => writer.write(offset, fieldData.getValueAsInt32)
        case INT64 => writer.write(offset, fieldData.getValueAsInt64)
        case FLOAT32 => writer.write(offset, fieldData.getValueAsFloat32)
        case FLOAT64 => writer.write(offset, fieldData.getValueAsFloat64)
        case DATE => fieldData.getValueAsDate.write(writer, offset, zoneId)
        case DATETIME => fieldData.getValueAsDatetime.write(writer, offset, zoneId)
        case TIME => fieldData.getValueAsTime.write(writer, offset, zoneId)
        case BOOL => writer.write(offset, fieldData.getValueAsBool)
        case STRING => writer.write(offset, UTF8String.fromString(fieldData.getValueAsString))
        case ENUMERATION => writer.write(offset, UTF8String.fromString(fieldData.getValueAsString))
        case SEQUENCE =>
          // TODO: assuming array of string only
          val elements = fieldData.numValues()
          if (elements == 0) {
            // Array might be empty resulting in writer not populated for that offset
            writer.writeStringArray(offset, Seq.empty[String])
          } else {
            writer.writeStringArray(offset, (0 until elements).map(index => {
              val item = fieldData.getValueAsElement(index)
              item.getElementAsString(item.name())
            }))
          }
        case _ => throw new IllegalStateException("Unsupported field type [" + fieldData.datatype + "]")
      }
    }

    def writeStringArray(offset: Int, fieldElements: Seq[String]): Unit = {
      val previousCursor = writer.cursor()
      val keyArrayWriter = new UnsafeArrayWriter(writer, getElementSize(StringType))
      keyArrayWriter.initialize(fieldElements.size)
      fieldElements.zipWithIndex.foreach({ case (k, i) => keyArrayWriter.write(i, UTF8String.fromString(k)) })
      writer.setOffsetAndSizeFromPreviousCursor(offset, previousCursor)
    }
  }

  /**
   * Get the number of bytes elements of a data type will occupy in the fixed part of an
   * [[UnsafeArrayData]] object. Reference types are stored as an 8 byte combination of an
   * offset (upper 4 bytes) and a length (lower 4 bytes), these point to the variable length
   * portion of the array object. Primitives take up to 8 bytes, depending on the size of the
   * underlying data type.
   */
  @tailrec
  private def getElementSize(dataType: DataType): Int = dataType match {
    case NullType | StringType | BinaryType | CalendarIntervalType |
         _: DecimalType | _: StructType | _: ArrayType | _: MapType => 8
    case udt: UserDefinedType[_] =>
      getElementSize(udt.sqlType)
    case _ => dataType.defaultSize
  }
}
