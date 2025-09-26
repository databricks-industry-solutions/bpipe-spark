package com.databricks.fsi.bpipe

import org.apache.spark.sql.types._

object BPipeFields {

  //TODO: remove and replace by dynamic lookup against //blp/fields (apifdls)
  object Request {
    val fields: Map[String, DataType] = Map(
      "PX_LAST" -> DoubleType,
      "LAST_PRICE" -> DoubleType,
      "BID" -> DoubleType,
      "ASK" -> DoubleType,
      "TICKER" -> StringType,
      "CHAIN_TICKERS" -> ArrayType(StringType),
      "TRADE_UPDATE_STAMP_RT" -> TimestampType,
      "LAST_UPDATE_BID_RT" -> TimestampType,
      "LAST_UPDATE_ASK_RT" -> TimestampType,
      "TIME" -> TimestampType,
      "MKTDATA_EVENT_TYPE" -> StringType,
      "MKTDATA_EVENT_SUBTYPE" -> StringType,
      "IS_DELAYED_STREAM" -> BooleanType,
      "EID" -> LongType,
      "NAME_RT" -> StringType,
      "SECURITY_DESCRIPTION_RT" -> StringType,
      "BID_SIZE" -> IntegerType,
      "ASK_SIZE" -> IntegerType,
      "SIZE_LAST_TRADE" -> IntegerType,
      "VOLUME" -> LongType
    )
  }

  object Response {

    val SECURITY = "SECURITY"
    val OPEN = "OPEN"
    val HIGH = "HIGH"
    val LOW = "LOW"
    val CLOSE = "CLOSE"
    val NUM_EVENTS = "NUM_EVENTS"
    val VOLUME = "VOLUME"
    val VALUE = "VALUE"
    val TIME = "TIME"
    val TYPE = "TYPE"
    val SIZE = "SIZE"

  }

}
