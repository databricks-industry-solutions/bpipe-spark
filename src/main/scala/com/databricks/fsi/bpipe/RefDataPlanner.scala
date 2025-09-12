package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.BPipeConfig._
import com.databricks.fsi.bpipe.RefDataHandler.RefDataPartitionReaderFactory
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.ZoneId
import scala.util.{Failure, Success, Try}

object RefDataPlanner {

  case class RefDataScan(
                          serviceName: String,
                          schema: StructType,
                          options: CaseInsensitiveStringMap
                        ) extends Scan {

    override def readSchema(): StructType = {
      schema
    }

    override def toBatch: Batch = {
      RefDataBatch(serviceName, schema, options)
    }

  }

  case class RefDataScanBuilder(
                                 serviceName: String,
                                 schema: StructType,
                                 options: CaseInsensitiveStringMap
                               ) extends ScanBuilder {

    override def build(): Scan = {
      RefDataScan(serviceName, schema, options)
    }
  }

  case class RefDataBatch(
                           serviceName: String,
                           schema: StructType,
                           options: CaseInsensitiveStringMap
                         ) extends Batch {

    final val apiConfig = RefDataApiConfig(options)
    final val svcConfig: SvcConfig = serviceName match {
      case HISTORICAL_DATA_REQUEST => RefDataHistoricalConfig(options)
      case REFERENCE_DATA_REQUEST => RefDataConfig(options)
      case INTRADAY_TICK_REQUEST => RefDataTickDataConfig(options)
      case INTRADAY_BAR_REQUEST => RefDataBarDataConfig(options)
      case _ => throw new IllegalArgumentException(s"Unsupported [serviceName] for [$BLP_REFDATA] service")
    }
    final val timezone = Try(ZoneId.of(options.getOrDefault("timezone", "UTC"))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("[timezone] should be valid timezone", e)
    }

    override def planInputPartitions(): Array[InputPartition] = {
      svcConfig.validate().buildPartitions(options)
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      RefDataPartitionReaderFactory(serviceName, apiConfig, schema, timezone)
    }
  }

}
