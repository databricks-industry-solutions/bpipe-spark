package com.databricks.fsi

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.{Failure, Success, Try}

package object bpipe {

  val BLP_REFDATA = "//blp/refdata"
  val BLP_STATICMKTDATA = "//blp/staticmktdata"
  val BLP_MKTDATA = "//blp/mktdata"

  val REFERENCE_DATA_REQUEST = "ReferenceDataRequest"
  val HISTORICAL_DATA_REQUEST = "HistoricalDataRequest"
  val INTRADAY_BAR_REQUEST = "IntradayBarRequest"
  val INTRADAY_TICK_REQUEST = "IntradayTickRequest"

  val BLP_REFDATA_SERVICES: Set[String] = Set(
    REFERENCE_DATA_REQUEST,
    HISTORICAL_DATA_REQUEST,
    INTRADAY_BAR_REQUEST,
    INTRADAY_TICK_REQUEST
  )

  val BLP_STATICMKTDATA_SERVICES: Set[String] = Set(
    REFERENCE_DATA_REQUEST
  )


  implicit class CaseInsensitiveStringMapImpl(options: CaseInsensitiveStringMap) {

    implicit val formats: DefaultFormats.type = DefaultFormats

    def getStringOpt(key: String): Option[String] = {
      if (options.containsKey(key)) Some(options.getString(key)) else None
    }

    def getString(key: String): String = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      options.get(key)
    }

    def getIntOpt(key: String): Option[Int] = {
      if (options.containsKey(key)) Some(options.getInt(key)) else None
    }

    def getInt(key: String): Int = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(options.getInt(key, -1)) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a valid integer", e)
      }
    }

    def getBooleanOpt(key: String): Option[Boolean] = {
      if (options.containsKey(key)) Some(options.getBoolean(key)) else None
    }

    def getBoolean(key: String): Boolean = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(options.getBoolean(key, false)) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a valid boolean", e)
      }
    }

    def getLongOpt(key: String): Option[Long] = {
      if (options.containsKey(key)) Some(options.getLong(key)) else None
    }

    def getLong(key: String): Long = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(options.getLong(key, -1L)) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a valid long", e)
      }
    }

    def getDoubleOpt(key: String): Option[Double] = {
      if (options.containsKey(key)) Some(options.getDouble(key)) else None
    }

    def getDouble(key: String): Double = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(options.getDouble(key, 0.0)) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a valid double", e)
      }
    }

    def getDateOpt(key: String): Option[Date] = {
      if (options.containsKey(key)) Some(options.getDate(key)) else None
    }

    def getDate(key: String): Date = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(sdf.parse(options.get(key))) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a date formatted as [yyyy-MM-dd]", e)
      }
    }

    def getTimestampOpt(key: String): Option[Date] = {
      if (options.containsKey(key)) Some(options.getTimestamp(key)) else None
    }

    def getTimestamp(key: String): Date = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try(sdf.parse(options.get(key))) match {
        case Success(i) => i
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be a timestamp formatted as [yyyy-MM-dd HH:mm:ss.SSS]", e)
      }
    }

    def getIntListOpt(key: String): List[Int] = {
      if (options.containsKey(key)) getIntList(key) else List.empty
    }

    def getIntList(key: String): List[Int] = {
      getComplexType[List[Int]](key)
    }

    def getStringListOpt(key: String): List[String] = {
      if (options.containsKey(key)) getStringList(key) else List.empty
    }

    def getStringList(key: String): List[String] = {
      getComplexType[List[String]](key)
    }

    def getComplexType[T](key: String)(implicit m: Manifest[T]): T = {
      require(options.containsKey(key), s"[$key] is not accessible in options")
      Try {
        parse(options.get(key).replace("'", "\"")).extract[T]
      } match {
        case Success(l) => l
        case Failure(e) =>
          throw new IllegalArgumentException(s"[$key] must be specified as JSON object", e)
      }
    }

    def getStringMapOpt(key: String): Map[String, String] = {
      if (options.containsKey(key)) getStringMap(key) else Map.empty
    }

    def getStringMap(key: String): Map[String, String] = {
      getComplexType[Map[String, String]](key)
    }

  }


}
