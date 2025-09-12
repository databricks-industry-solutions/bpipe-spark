package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.BPipeConfig._
import com.databricks.fsi.bpipe.StaticMktDataHandler.StaticMktDataPartitionReaderFactory
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.ZoneId
import scala.util.{Failure, Success, Try}

object StaticMktDataPlanner {

  case class StaticMktDataScan(
                                serviceName: String,
                                schema: StructType,
                                options: CaseInsensitiveStringMap
                              ) extends Scan {

    override def readSchema(): StructType = {
      schema
    }

    override def toBatch: Batch = {
      StaticMktDataBatch(serviceName, schema, options)
    }

  }

  case class StaticMktDataScanBuilder(
                                       serviceName: String,
                                       schema: StructType,
                                       options: CaseInsensitiveStringMap
                                     ) extends ScanBuilder {

    override def build(): Scan = {
      StaticMktDataScan(serviceName, schema, options)
    }
  }

  case class StaticMktDataBatch(
                                 serviceName: String,
                                 schema: StructType,
                                 options: CaseInsensitiveStringMap
                               ) extends Batch {

    final val apiConfig = StaticMktDataApiConfig(options)
    final val svcConfig: SvcConfig = serviceName match {
      case REFERENCE_DATA_REQUEST => StaticMktDataConfig(options)
      case _ => throw new IllegalArgumentException(s"Unsupported [serviceName] for [$BLP_STATICMKTDATA] service")
    }
    final val timezone = Try(ZoneId.of(options.getOrDefault("timezone", "UTC"))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("[timezone] should be valid timezone", e)
    }

    override def planInputPartitions(): Array[InputPartition] = {
      svcConfig.validate().buildPartitions(options)
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      StaticMktDataPartitionReaderFactory(serviceName, apiConfig, schema, timezone)
    }
  }

}
