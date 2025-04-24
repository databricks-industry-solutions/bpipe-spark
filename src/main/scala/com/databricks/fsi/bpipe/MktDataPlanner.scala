package com.databricks.fsi.bpipe

import com.databricks.fsi.bpipe.BPipeConfig.{ApiConfig, MktDataConfig}
import com.databricks.fsi.bpipe.MktDataHandler.MktDataPartitionReaderFactory
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.ZoneId
import scala.util.{Failure, Success, Try}

object MktDataPlanner {

  case class MktDataScan(
                          schema: StructType,
                          options: CaseInsensitiveStringMap
                        ) extends Scan {

    override def readSchema(): StructType = {
      schema
    }

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      MktDataMicroBatchStream(schema, options)
    }

  }

  case class MktDataScanBuilder(
                                 schema: StructType,
                                 options: CaseInsensitiveStringMap
                               ) extends ScanBuilder {

    override def build(): Scan = {
      MktDataScan(schema, options)
    }
  }

  case class MktDataMicroBatchStream(
                                      schema: StructType,
                                      options: CaseInsensitiveStringMap
                                    ) extends MicroBatchStream {

    final val apiConfig = ApiConfig(options)
    final val mktConfig = MktDataConfig(options)
    final val timezone = Try(ZoneId.of(options.getOrDefault("timezone", "UTC"))) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("[timezone] should be valid timezone", e)
    }

    override def latestOffset(): Offset = {
      // No fail-over on B-PIPE, can't subscribe from previous offset
      new LongOffset(System.currentTimeMillis())
    }

    override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
      mktConfig.validate().buildPartitions(options)
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      MktDataPartitionReaderFactory(apiConfig, schema, timezone)
    }

    override def initialOffset(): Offset = {
      // No fail-over on B-PIPE, can't subscribe from previous offset
      new LongOffset(0L)
    }

    override def deserializeOffset(json: String): Offset = {
      // No fail-over on B-PIPE, can't subscribe from previous offset
      new LongOffset(0L)
    }

    override def commit(end: Offset): Unit = {
      // No fail-over on B-PIPE, can't subscribe from previous offset
    }

    override def stop(): Unit = {

    }

  }


}
