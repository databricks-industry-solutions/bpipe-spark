package com.databricks.fsi.bpipe

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class StaticMktDataSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val serviceName = options.getString("serviceName")
    serviceName match {
      case REFERENCE_DATA_REQUEST => inferReferenceDataSchema(options)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported [serviceName] for [$BLP_STATICMKTDATA] service")
    }
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

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = {

    val serviceName = properties.getOrDefault("serviceName", "UNKNOWN")
    require(BLP_STATICMKTDATA_SERVICES.contains(serviceName), s"Unsupported [serviceName] for [$BLP_STATICMKTDATA] service")
    StaticMktDataTable(serviceName, schema)

  }

  override def shortName(): String = {
    BLP_STATICMKTDATA
  }
}
