package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi.{Datetime, Element, Name, Request}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.annotation.tailrec

object BPipeConfig {

  private val validEventTypes = Set(
    "TRADE",
    "BID",
    "ASK",
    "BID_BEST",
    "ASK_BEST",
    "MID_PRICE",
    "AT_TRADE",
    "SETTLE",
    "BEST_BID",
    "BEST_ASK"
  )
  private val validPeriodicityAdjustment = Set(
    "ACTUAL",
    "CALENDAR",
    "FISCAL"
  )
  private val validPeriodicitySelection = Set(
    "DAILY",
    "WEEKLY",
    "MONTHLY",
    "QUARTERLY",
    "SEMI_ANNUALLY",
    "YEARLY"
  )
  private val validPricingOption = Set(
    "PRICING_OPTION_PRICE",
    "PRICING_OPTION_YIELD"
  )
  private val validOverrideOption = Set(
    "OVERRIDE_OPTION_GPA",
    "OVERRIDE_OPTION_CLOSE"
  )
  private val LOGGER = LoggerFactory.getLogger(BPipeConfig.getClass)

  /**
   * Use might define partitioning strategy to leverage distributed nature of Spark Streaming. With multiple nodes
   * could come multiple requests where each request only processes specific securities
   * @param options arguments of a spark reader, in form of case insensitive options
   * @param securities list of securities to be processed
   * @return the list of securities that will be processed in each partition
   */
  private[bpipe] def partitionBySecurities(
                                            options: CaseInsensitiveStringMap,
                                            securities: List[String]
                                          ): Array[List[String]] = {

    if (options.containsKey("partitions")) {
      val stringPartitions = options.getString("partitions")

      if (stringPartitions.matches("\\d+")) {
        // Naive partitioning, we have X nodes, we run X requests in parallel
        val numPartitions = options.getInt("partitions")
        require(numPartitions >= 1, "[partitions] should be greater than 1")
        LOGGER.info(s"Using naive partitioning with ${securities.size} securities and $numPartitions partitions")
        securities.groupBy(sec => math.abs(sec.hashCode % numPartitions)).values.toArray
      } else {
        // Smart partitioning, we know we want to run specific securities against specific requests
        // Some more liquid assets might consume most of a node resource while less liquid could be combined
        // in a same request
        val partitions = options.getIntListOpt("partitions")
        require(partitions.size == securities.length, "[partitions] must be of same size as [securities]")
        LOGGER.info(s"Using smart partitioning with ${securities.size} securities " +
          s"and ${partitions.distinct.size} partitions")
        partitions.zip(securities).groupBy(_._1).values.map(_.map(_._2)).toArray
      }

    } else {
      // No partitioning, all securities will be handled by one executor and multiple cores
      LOGGER.warn(s"No partitioning defined for ${securities.size} securities, consider using option [partitions]")
      Array(securities)
    }

  }

  @tailrec
  private[bpipe] def partitionDate(
                                    date: Long,
                                    endDate: Long,
                                    windowSize: Long,
                                    processed: Array[(Date, Date)] = Array.empty
                                  ): Array[(Date, Date)] = {

    val newEndDate = date + windowSize
    if (newEndDate >= endDate)
      return processed :+ ((new Date(date), new Date(endDate)))
    partitionDate(
      date + windowSize,
      endDate,
      windowSize,
      processed :+ ((new Date(date), new Date(newEndDate)))
    )
  }

  /**
   * Use might define partitioning strategy to leverage distributed nature of Spark Streaming. With multiple nodes
   * could come multiple requests where each request only processes a specific time window
   * @param options arguments of a spark reader, in form of case insensitive options
   * @param startDate the start time for the given historical data request
   * @param endDate the end time for the given historical data request
   * @return the windowed start / end times that will be processed within each partition
   */
  private[bpipe] def partitionByDate(
                                      options: CaseInsensitiveStringMap,
                                      startDate: Date,
                                      endDate: Date
                                    ): Array[(Date, Date)] = {

    val startTime = startDate.getTime
    val endTime = endDate.getTime
    val partitions = options.getIntOpt("partitions")
    if (partitions.isDefined) {
      require(partitions.get >= 1, "[partitions] should be greater than 1")
      LOGGER.info(s"Using date partitioning with ${partitions.get} partitions")
      val partitionSize = (endTime - startTime) / partitions.get
      // Recursive call to split time window into multiple intervals
      partitionDate(startTime, endTime, partitionSize)
    } else {
      // No partitioning, entire time window will be handled by one executor and multiple cores
      LOGGER.warn(s"No partitioning defined, consider using option [partitions]")
      Array((startDate, endDate))
    }
  }

  trait SvcConfig extends InputPartition with Serializable {

    def buildRequest(request: Request): Unit

    def validate(): SvcConfig

    def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition]

  }

  case class ApiConfig(
                        serviceHost: String,
                        servicePort: Int,
                        correlationId: Long
                      ) extends Serializable

  case class MktDataConfig(
                            securities: List[String] = List.empty,
                            fields: List[String] = List.empty
                          ) extends SvcConfig {

    override def buildRequest(request: Request): Unit = {
      throw new IllegalAccessError("Unsupported")
    }

    override def validate(): MktDataConfig = {
      // Those are mandatory fields for market data request
      require(securities.nonEmpty, "[securities] needs to be specified")
      require(fields.nonEmpty, "[fields] needs to be specified")
      this
    }

    override def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition] = {
      LOGGER.info("Partitioning Market subscriptions")
      // Market data can be partitioned by securities, where each partition (a spark task) responsible for
      // only a subset of securities to fetch
      partitionBySecurities(options, securities).map(securities => {
        this.copy(securities = securities)
      })
    }

  }

  /**
   * Helper class to ensure BPipe request is built consistantly across multiple entry points
   * @param request original request modified with input options like securities, fields, start or end date
   */
  implicit class RequestImpl(request: Request) {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    def appendFields(fields: List[String]): Unit = {
      fields.foreach(f => request.append(Name.getName("fields"), f))
    }

    def appendSecurities(securities: List[String]): Unit = {
      securities.foreach(s => request.append(Name.getName("securities"), s))
    }

    def appendEventTypes(eventTypes: List[String]): Unit = {
      eventTypes.foreach(e => request.append(Name.getName("eventTypes"), e))
    }

    def setSecurity(security: String): Unit = {
      request.set(Name.getName("security"), security)
    }

    def setStartDateTime(startDateTime: Date): Unit = {
      val startCalendar = Calendar.getInstance
      startCalendar.setTime(startDateTime)
      request.set(Name.getName("startDateTime"), new Datetime(startCalendar))
    }

    def setEndDateTime(endDateTime: Date): Unit = {
      val endCalendar = Calendar.getInstance
      endCalendar.setTime(endDateTime)
      request.set(Name.getName("endDateTime"), new Datetime(endCalendar))
    }

    def setStartDate(startDate: Date): Unit = {
      request.set(Name.getName("startDate"), sdf.format(startDate))
    }

    def setEndDate(endDate: Date): Unit = {
      request.set(Name.getName("endDate"), sdf.format(endDate))
    }

    def setInterval(interval: Int): Unit = {
      request.set(Name.getName("interval"), interval)
    }

  }

  case class RefDataConfig(
                            securities: List[String] = List.empty,
                            fields: List[String] = List.empty,
                            overrides: Map[String, String] = Map.empty
                          ) extends SvcConfig {

    override def buildRequest(request: Request): Unit = {

      request.appendFields(fields)
      request.appendSecurities(securities)
      if (overrides.nonEmpty) {
        val overridesElements: Element = request.getElement(Name.getName("overrides"))
        overrides.foreach({ case (overrideKey, overrideValue) =>
          val overridesElement: Element = overridesElements.appendElement
          overridesElement.setElement(Name.getName("fieldId"), overrideKey)
          overridesElement.setElement(Name.getName("value"), overrideValue)
        })
      }
    }

    override def validate(): RefDataConfig = {
      require(securities.nonEmpty, "[securities] needs to be specified")
      require(fields.nonEmpty, "[fields] needs to be specified")
      //TODO: We should validate overrides parameters (both keys and values)
      this
    }

    override def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition] = {
      LOGGER.info("Partitioning Reference data requests")
      partitionBySecurities(options, securities).map(securities => {
        this.copy(securities = securities)
      })
    }

  }

  case class RefDataHistoricalConfig(
                                      securities: List[String] = List.empty,
                                      fields: List[String] = List.empty,
                                      startDate: Date,
                                      endDate: Date = new Date(),
                                      periodicityAdjustment: Option[String],
                                      periodicitySelection: Option[String],
                                      pricingOption: Option[String],
                                      adjustmentNormal: Option[Boolean],
                                      adjustmentAbnormal: Option[Boolean],
                                      adjustmentSplit: Option[Boolean],
                                      maxDataPoints: Option[Int],
                                      overrideOption: Option[String]
                                    ) extends SvcConfig {

    override def buildRequest(request: Request): Unit = {

      request.appendFields(fields)
      request.appendSecurities(securities)
      request.setStartDate(startDate)
      request.setEndDate(endDate)

      if (periodicityAdjustment.isDefined) request.set(Name.getName("periodicityAdjustment"), periodicityAdjustment.get)
      if (periodicitySelection.isDefined) request.set(Name.getName("periodicitySelection"), periodicitySelection.get)
      if (pricingOption.isDefined) request.set(Name.getName("pricingOption"), pricingOption.get)
      if (adjustmentNormal.isDefined) request.set(Name.getName("adjustmentNormal"), adjustmentNormal.get)
      if (adjustmentAbnormal.isDefined) request.set(Name.getName("adjustmentAbnormal"), adjustmentAbnormal.get)
      if (adjustmentSplit.isDefined) request.set(Name.getName("adjustmentSplit"), adjustmentSplit.get)
      if (maxDataPoints.isDefined) request.set(Name.getName("maxDataPoints"), maxDataPoints.get)
      if (overrideOption.isDefined) request.set(Name.getName("overrideOption"), overrideOption.get)

    }

    override def validate(): RefDataHistoricalConfig = {

      require(securities.nonEmpty, "[securities] needs to be specified")
      require(fields.nonEmpty, "[fields] needs to be specified")
      require(startDate != null, "[startDate] needs to be specified")
      require(startDate.before(new Date()), "[startDate] needs to be in the past")
      require(startDate.before(endDate), "[startDate] needs to be before [endDate]")

      if (maxDataPoints.isDefined)
        require(maxDataPoints.get > 1, s"[maxDataPoints] must be a positive integer")
      if (periodicityAdjustment.isDefined)
        require(validPeriodicityAdjustment.contains(periodicityAdjustment.get),
          s"[periodicityAdjustment] must be one of [${validPeriodicityAdjustment.mkString(",")}]")
      if (periodicitySelection.isDefined)
        require(validPeriodicitySelection.contains(periodicitySelection.get),
          s"[periodicitySelection] must be one of [${validPeriodicitySelection.mkString(",")}]")
      if (pricingOption.isDefined)
        require(validPricingOption.contains(pricingOption.get),
          s"[pricingOption] must be one of [${validPricingOption.mkString(",")}]")
      if (overrideOption.isDefined)
        require(validOverrideOption.contains(overrideOption.get),
          s"[overrideOption] must be one of [${validOverrideOption.mkString(",")}]")
      this
    }

    override def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition] = {
      LOGGER.info("Partitioning Historical data requests")
      if (securities.size == 1) {
        partitionByDate(options, startDate, endDate).map(window => {
          this.copy(startDate = window._1, endDate = window._2)
        })
      } else {
        partitionBySecurities(options, securities).map(securities => {
          this.copy(securities = securities)
        })
      }
    }

  }

  case class RefDataTickDataConfig(
                                    security: String,
                                    eventTypes: List[String] = List.empty,
                                    startDateTime: Date,
                                    endDateTime: Date = new Date(),
                                    returnEids: Option[Boolean] = None,
                                    includeConditionCodes: Option[Boolean] = None,
                                    includeExchangeCodes: Option[Boolean] = None,
                                    includeNonPlottableEvents: Option[Boolean] = None,
                                    includeBrokerCodes: Option[Boolean] = None,
                                    includeRpsCodes: Option[Boolean] = None,
                                    includeBicMicCodes: Option[Boolean] = None
                                  ) extends SvcConfig {

    override def buildRequest(request: Request): Unit = {

      request.set(Name.getName("security"), security)
      request.setSecurity(security)
      request.appendEventTypes(eventTypes)
      request.setStartDateTime(startDateTime)
      request.setEndDateTime(endDateTime)

      if (includeConditionCodes.isDefined) request.set(Name.getName("includeConditionCodes"), includeConditionCodes.get)
      if (includeBicMicCodes.isDefined) request.set(Name.getName("includeBicMicCodes"), includeBicMicCodes.get)
      if (includeBrokerCodes.isDefined) request.set(Name.getName("includeBrokerCodes"), includeBrokerCodes.get)
      if (includeRpsCodes.isDefined) request.set(Name.getName("includeRpsCodes"), includeRpsCodes.get)
      if (includeExchangeCodes.isDefined) request.set(Name.getName("includeExchangeCodes"), includeExchangeCodes.get)
      if (includeNonPlottableEvents.isDefined) request.set(Name.getName("includeNonPlottableEvents"), includeNonPlottableEvents.get)
      if (returnEids.isDefined) request.set(Name.getName("returnEids"), returnEids.get)
    }

    override def validate(): RefDataTickDataConfig = {
      require(StringUtils.isNotEmpty(security), "[security] needs to be specified")
      require(startDateTime != null, "[startDateTime] needs to be specified")
      require(startDateTime.before(new Date()), "[startDateTime] needs to be in the past")
      require(startDateTime.before(endDateTime), "[startDateTime] needs to be before [endDateTime]")
      require(eventTypes.forall(eventType =>
        validEventTypes.contains(eventType)), s"[eventTypes] must be [${validEventTypes.mkString(", ")}]")
      this
    }

    override def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition] = {
      LOGGER.info("Partitioning Tick data requests")
      partitionByDate(options, startDateTime, endDateTime).map({ case (start, end) =>
        this.copy(startDateTime = start, endDateTime = end)
      })
    }

  }

  case class RefDataBarDataConfig(
                                   security: String,
                                   startDateTime: Date,
                                   endDateTime: Date = new Date(),
                                   interval: Int,
                                   eventType: Option[String] = None,
                                   returnEids: Option[Boolean] = None,
                                   gapFillInitialBar: Option[Boolean] = None,
                                   adjustmentNormal: Option[Boolean] = None,
                                   adjustmentAbnormal: Option[Boolean] = None,
                                   adjustmentSplit: Option[Boolean] = None,
                                   adjustmentFollowDPDF: Option[Boolean] = None
                                 ) extends SvcConfig {

    override def buildRequest(request: Request): Unit = {

      request.setSecurity(security)
      request.setStartDateTime(startDateTime)
      request.setEndDateTime(endDateTime)
      request.setInterval(interval)

      if (eventType.isDefined) request.set(Name.getName("eventType"), eventType.get)
      if (returnEids.isDefined) request.set(Name.getName("returnEids"), returnEids.get)
      if (gapFillInitialBar.isDefined) request.set(Name.getName("gapFillInitialBar"), gapFillInitialBar.get)
      if (adjustmentNormal.isDefined) request.set(Name.getName("adjustmentNormal"), adjustmentNormal.get)
      if (adjustmentAbnormal.isDefined) request.set(Name.getName("adjustmentAbnormal"), adjustmentAbnormal.get)
      if (adjustmentSplit.isDefined) request.set(Name.getName("adjustmentSplit"), adjustmentSplit.get)
      if (adjustmentFollowDPDF.isDefined) request.set(Name.getName("adjustmentFollowDPDF"), adjustmentFollowDPDF.get)

    }

    override def validate(): RefDataBarDataConfig = {
      require(StringUtils.isNotEmpty(security), "[security] needs to be specified")
      require(startDateTime != null, "[startDateTime] needs to be specified")
      require(startDateTime.before(new Date()), "[startDateTime] needs to be in the past")
      require(startDateTime.before(endDateTime), "[startDateTime] needs to be before [endDateTime]")
      require(interval >= 1 && interval <= 1440, "[interval] needs to be between 1 and 1440 in minutes")
      if (eventType.isDefined)
        require(validEventTypes.contains(eventType.get), s"[eventType] must be [${validEventTypes.mkString(", ")}]")
      this
    }

    override def buildPartitions(options: CaseInsensitiveStringMap): Array[InputPartition] = {
      LOGGER.info("Partitioning Bar data requests")
      partitionByDate(options, startDateTime, endDateTime).map({ case (start, end) =>
        this.copy(startDateTime = start, endDateTime = end)
      })
    }

  }

  object ApiConfig {
    def apply(options: CaseInsensitiveStringMap): ApiConfig = {
      ApiConfig(
        options.getString("serviceHost"),
        options.getInt("servicePort"),
        options.getLong("correlationId")
      )
    }
  }

  object MktDataConfig {
    def apply(options: CaseInsensitiveStringMap): MktDataConfig = {
      MktDataConfig(
        options.getStringList("securities"),
        options.getStringList("fields")
      )
    }
  }

  object RefDataConfig {
    def apply(options: CaseInsensitiveStringMap): RefDataConfig = {
      RefDataConfig(
        options.getStringList("securities"),
        options.getStringList("fields"),
        options.getStringMapOpt("overrides")
      )
    }
  }

  object RefDataTickDataConfig {
    def apply(options: CaseInsensitiveStringMap): RefDataTickDataConfig = {
      RefDataTickDataConfig(
        options.getString("security"),
        options.getStringListOpt("eventTypes"),
        options.getDate("startDateTime"),
        options.getDateOpt("endDateTime").getOrElse(new Date()),
        options.getBooleanOpt("returnEids"),
        options.getBooleanOpt("includeConditionCodes"),
        options.getBooleanOpt("includeExchangeCodes"),
        options.getBooleanOpt("includeNonPlottableEvents"),
        options.getBooleanOpt("includeBrokerCodes"),
        options.getBooleanOpt("includeRpsCodes"),
        options.getBooleanOpt("includeBicMicCodes")
      )
    }
  }

  object RefDataBarDataConfig {
    def apply(options: CaseInsensitiveStringMap): RefDataBarDataConfig = {
      RefDataBarDataConfig(
        options.getString("security"),
        options.getDate("startDateTime"),
        options.getDateOpt("endDateTime").getOrElse(new Date()),
        options.getInt("interval"),
        options.getStringOpt("eventType"),
        options.getBooleanOpt("returnEids"),
        options.getBooleanOpt("gapFillInitialBar"),
        options.getBooleanOpt("adjustmentNormal"),
        options.getBooleanOpt("adjustmentAbnormal"),
        options.getBooleanOpt("adjustmentSplit"),
        options.getBooleanOpt("adjustmentFollowDPDF")
      )
    }
  }

  object RefDataHistoricalConfig {
    def apply(options: CaseInsensitiveStringMap): RefDataHistoricalConfig = {
      RefDataHistoricalConfig(
        options.getStringList("securities"),
        options.getStringList("fields"),
        options.getDate("startDate"),
        options.getDateOpt("endDate").getOrElse(new Date()),
        options.getStringOpt("periodicityAdjustment"),
        options.getStringOpt("periodicitySelection"),
        options.getStringOpt("pricingOption"),
        options.getBooleanOpt("adjustmentNormal"),
        options.getBooleanOpt("adjustmentAbnormal"),
        options.getBooleanOpt("adjustmentSplit"),
        options.getIntOpt("maxDataPoints"),
        options.getStringOpt("overrideOption")
      )
    }
  }

}
