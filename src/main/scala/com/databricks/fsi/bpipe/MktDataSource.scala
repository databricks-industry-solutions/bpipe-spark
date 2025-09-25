package com.databricks.fsi.bpipe

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class MktDataSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(
      StructField(BPipeFields.Response.SECURITY, StringType, nullable = true) +:
        options.getStringList("fields").map(field => {
          if (BPipeFields.Request.fields.contains(field)) {
            // TODO: Establish initial connection to B-PIPE (//blp/fields) to get access to field specification
            StructField(field, BPipeFields.Request.fields(field), nullable = true)
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
                       ): Table = MktDataTable(schema)

  override def shortName(): String = {
    BLP_MKTDATA
  }
}
