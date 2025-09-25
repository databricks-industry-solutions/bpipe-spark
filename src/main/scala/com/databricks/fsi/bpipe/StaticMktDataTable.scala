package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.StaticMktDataPlanner.StaticMktDataScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

case class StaticMktDataTable(
                               serviceName: String,
                               structType: StructType
                             ) extends SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    StaticMktDataScanBuilder(serviceName, structType, options)
  }

  override def name(): String = {
    BLP_STATICMKTDATA + "/" + serviceName
  }

  override def schema(): StructType = {
    structType
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }
}
