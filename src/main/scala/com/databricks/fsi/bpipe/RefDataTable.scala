package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.RefDataPlanner.RefDataScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

case class RefDataTable(
                         serviceName: String,
                         structType: StructType
                       ) extends SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    RefDataScanBuilder(serviceName, structType, options)
  }

  override def name(): String = {
    BLP_REFDATA + "/" + serviceName
  }

  override def schema(): StructType = {
    structType
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }
}
