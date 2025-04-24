package com.databricks.fsi.bpipe

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class RefDataSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val serviceName = options.getString("serviceName")
    serviceName match {
      case HISTORICAL_DATA_REQUEST => inferHistoricalDataSchema(options)
      case REFERENCE_DATA_REQUEST => inferReferenceDataSchema(options)
      case INTRADAY_TICK_REQUEST => inferIntradayTickSchema(options)
      case INTRADAY_BAR_REQUEST => inferIntradayBarSchema(options)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported [serviceName] for [$BLP_REFDATA] service")
    }
  }

  def inferIntradayBarSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(Seq(
      StructField(BPipeFields.Response.SECURITY, StringType, nullable = false),
      StructField(BPipeFields.Response.TIME, TimestampType, nullable = false),
      StructField(BPipeFields.Response.OPEN, DoubleType, nullable = false),
      StructField(BPipeFields.Response.HIGH, DoubleType, nullable = false),
      StructField(BPipeFields.Response.LOW, DoubleType, nullable = false),
      StructField(BPipeFields.Response.CLOSE, DoubleType, nullable = false),
      StructField(BPipeFields.Response.NUM_EVENTS, LongType, nullable = false),
      StructField(BPipeFields.Response.VOLUME, LongType, nullable = false),
      StructField(BPipeFields.Response.VALUE, DoubleType, nullable = false)
    ))
  }

  def inferIntradayTickSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(Seq(
      StructField(BPipeFields.Response.SECURITY, StringType, nullable = false),
      StructField(BPipeFields.Response.TIME, TimestampType, nullable = false),
      StructField(BPipeFields.Response.TYPE, StringType, nullable = false),
      StructField(BPipeFields.Response.VALUE, DoubleType, nullable = false),
      StructField(BPipeFields.Response.SIZE, LongType, nullable = false)
    ))
  }

  def inferReferenceDataSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(
      StructField(BPipeFields.Response.SECURITY, StringType, nullable = false) +:
        options.getStringList("fields").map(field => {
          if (BPipeFields.Request.fields.contains(field)) {
            // TODO: Establish initial connection to B-PIPE (//blp/fields) to get access to field specification
            StructField(field, BPipeFields.Request.fields(field), nullable = false)
          } else {
            throw new IllegalArgumentException(s"[fields] not supported $field")
          }
        })
    )
  }

  def inferHistoricalDataSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(
      Seq(
        StructField(BPipeFields.Response.SECURITY, StringType, nullable = false),
        StructField(BPipeFields.Response.TIME, TimestampType, nullable = false)
      ) ++
        options.getStringList("fields").map(field => {
          if (BPipeFields.Request.fields.contains(field)) {
            // TODO: Establish initial connection to B-PIPE (//blp/fields) to get access to field specification
            StructField(field, BPipeFields.Request.fields(field), nullable = false)
          } else {
            throw new IllegalArgumentException(s"[fields] not supported $field")
          }
        })
    )
  }

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = {

    val serviceName = properties.getOrDefault("serviceName", "UNKNOWN")
    require(BLP_REFDATA_SERVICES.contains(serviceName), s"Unsupported [serviceName] for [$BLP_REFDATA] service")
    RefDataTable(serviceName, schema)

  }

  override def shortName(): String = {
    BLP_REFDATA
  }
}
