package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi._
import com.databricks.fsi.bpipe.BPipeConfig._
import com.databricks.fsi.bpipe.BPipeUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.time.ZoneId
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object RefDataHandler {

  case class RefDataPartitionReaderFactory(
                                            serviceName: String,
                                            apiConfig: ApiConfig,
                                            schema: StructType,
                                            timezone: ZoneId
                                          ) extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      RefDataPartitionReader(serviceName, schema, apiConfig, partition.asInstanceOf[SvcConfig], timezone)
    }
  }

  case class RefDataPartitionReader(
                                     serviceName: String,
                                     schema: StructType,
                                     apiConfig: ApiConfig,
                                     svcConfig: SvcConfig,
                                     timezone: ZoneId
                                   ) extends PartitionReader[InternalRow] {

    private val LOGGER = LoggerFactory.getLogger(this.getClass)
    private var session: Session = _
    private var iterator: Iterator[UnsafeRow] = Iterator.empty
    private var isPartialResponse: Boolean = false

    Try {

      // Instantiate new session
      LOGGER.info("Starting new B-PIPE session")
      val sessionOptions = new SessionOptions
      sessionOptions.setServerHost(apiConfig.serviceHost)
      sessionOptions.setServerPort(apiConfig.servicePort)
      // TODO: Pass application ID. Not supported in BEMU emulator
      session = new Session(sessionOptions)
      session.start

      // Open service to tick data API
      LOGGER.info(s"Opening service [$BLP_REFDATA]")
      session.openService(BLP_REFDATA)
      val service = session.getService(BLP_REFDATA)

      // Create a new request
      LOGGER.info(s"Creating request for [$serviceName]")
      val request = service.createRequest(serviceName)
      svcConfig.buildRequest(request)

      // Send request to B-PIPE API
      LOGGER.info(s"Publishing request for [$serviceName]")
      session.sendRequest(request, new CorrelationID(apiConfig.correlationId))

      // Ensure our first batch of record is set for consumption
      isPartialResponse = getNextResponseIterator

    } match {
      case Success(_) =>
      case Failure(exception) =>
        LOGGER.error(s"[B-PIPE connection error]: ${exception.getMessage}")
        throw new IllegalStateException("[B-PIPE connection error]", exception)
    }

    def hasResponseError(message: Message): Boolean = {
      if (message.hasElement(Name.getName("responseError"), true)) {
        val secError = message.getElement(Name.getName("responseError"))
        val errorMessage = secError.getElementAsString(Name.getName("message"))
        LOGGER.error(s"[B-PIPE response error]: $errorMessage")
        true
      } else false
    }

    def hasDataError(securityData: Element): Boolean = {
      if (securityData.hasElement(Name.getName("securityError"), true)) {
        val securityError = securityData.getElement(Name.getName("securityError"))
        val message = securityError.getElementAsString(Name.getName("message"))
        LOGGER.error(s"[B-PIPE data error]: $message")
        true
      } else false
    }

    def hasFieldError(securityData: Element): Boolean = {
      if (securityData.hasElement(Name.getName("fieldExceptions"), true)) {
        val fieldExceptions = securityData.getElement(Name.getName("fieldExceptions"))
        val messages = (0 until fieldExceptions.numValues()).map(i => {
          val fieldError = fieldExceptions.getValueAsElement(i)
          val errorInfo = fieldError.getElement(Name.getName("errorInfo"))
          errorInfo.getElementAsString(Name.getName("message"))
        }).mkString("\n")
        LOGGER.error(s"[B-PIPE fields error]: $messages")
        true
      } else false
    }

    def processTickDataResponse(event: Event): Unit = {
      val security = svcConfig.asInstanceOf[RefDataTickDataConfig].security
      iterator = event.messageIterator.asScala.flatMap(message => {
        if (hasResponseError(message)) None else {

          val tickData = message.getElement(Name.getName("tickData"))
          val tickDataArray = tickData.getElement(Name.getName("tickData"))

          (0 until tickDataArray.numValues).map(i => {
            val tickDataItem = tickDataArray.getValueAsElement(i)
            val writer = new UnsafeRowWriter(schema.length)
            writer.resetRowWriter()

            val fieldValues = Map(
              BPipeFields.Response.TIME -> tickDataItem.getElement(Name.getName("time")),
              BPipeFields.Response.TYPE -> tickDataItem.getElement(Name.getName("type")),
              BPipeFields.Response.VALUE -> tickDataItem.getElement(Name.getName("value")),
              BPipeFields.Response.SIZE -> tickDataItem.getElement(Name.getName("size"))
            )

            schema.fields.zipWithIndex.foreach({ case (requiredField, requiredIndex) =>
              requiredField.name match {
                case BPipeFields.Response.SECURITY => writer.write(requiredIndex, UTF8String.fromString(security))
                case _ => if (fieldValues.contains(requiredField.name)) {
                  writer.writeElement(requiredIndex, fieldValues(requiredField.name), timezone)
                } else {
                  requiredField.dataType match {
                    case ArrayType(StringType, true) => writer.writeStringArray(requiredIndex, Seq.empty[String])
                    case _ => writer.setNullAt(requiredIndex)
                  }
                }
              }
            })
            writer.getRow
          })
        }
      })
    }

    def processRefDataResponse(event: Event): Unit = {
      iterator = event.messageIterator.asScala.flatMap(message => {
        val securityDataArray = message.getElement(Name.getName("securityData"))
        (0 until securityDataArray.numValues).flatMap(i => {
          val securityData = securityDataArray.getValueAsElement(i)
          if (hasDataError(securityData) || hasFieldError(securityData)) None else {

            val security = securityData.getElementAsString(Name.getName("security"))
            val fieldDataArray = securityData.getElement(Name.getName("fieldData"))
            val writer = new UnsafeRowWriter(schema.length)
            writer.resetRowWriter()

            val fieldValues = (0 until fieldDataArray.numElements).map(j => {
              val fieldData = fieldDataArray.getElement(j)
              val fieldName = fieldData.name.toString
              (fieldName, fieldData)
            }).toMap

            schema.fields.zipWithIndex.foreach({ case (requiredField, requiredIndex) =>
              requiredField.name match {
                case BPipeFields.Response.SECURITY => writer.write(requiredIndex, UTF8String.fromString(security))
                case _ => if (fieldValues.contains(requiredField.name)) {
                  writer.writeElement(requiredIndex, fieldValues(requiredField.name), timezone)
                } else {
                  requiredField.dataType match {
                    case ArrayType(StringType, true) => writer.writeStringArray(requiredIndex, Seq.empty[String])
                    case _ => writer.setNullAt(requiredIndex)
                  }
                }
              }
            })
            Some(writer.getRow)
          }
        })
      })
    }

    def processHistoricalDataResponse(event: Event): Unit = {
      iterator = event.messageIterator.asScala.flatMap(message => {
        val securityData = message.getElement(Name.getName("securityData"))
        if (hasDataError(securityData) || hasFieldError(securityData)) None else {

          val security = securityData.getElementAsString(Name.getName("security"))
          val fieldDataArray = securityData.getElement(Name.getName("fieldData"))

          (0 until fieldDataArray.numValues()).map(i => {

            val fieldValues = fieldDataArray.getValueAsElement(i)
            val date = fieldValues.getElementAsDate(Name.getName("date"))
            val writer = new UnsafeRowWriter(schema.length)
            writer.resetRowWriter()

            schema.fields.zipWithIndex.foreach({ case (requiredField, offset) =>
              requiredField.name match {
                case BPipeFields.Response.SECURITY => writer.write(offset, UTF8String.fromString(security))
                case BPipeFields.Response.TIME => date.write(writer, offset, timezone)
                case _ =>
                  // TODO: Emulator does not map getElement, we do not know what type it is.
                  if (fieldValues.hasElement(Name.getName(requiredField.name))) {
                    requiredField.dataType match {
                      case FloatType => writer.write(offset, fieldValues.getElementAsFloat32(Name.getName(requiredField.name)))
                      case DoubleType => writer.write(offset, fieldValues.getElementAsFloat64(Name.getName(requiredField.name)))
                      case IntegerType => writer.write(offset, fieldValues.getElementAsInt32(Name.getName(requiredField.name)))
                      case LongType => writer.write(offset, fieldValues.getElementAsInt64(Name.getName(requiredField.name)))
                      case StringType => writer.write(offset, UTF8String.fromString(fieldValues.getElementAsString(Name.getName(requiredField.name))))
                      case DateType => fieldValues.getElementAsDate(Name.getName(requiredField.name)).write(writer, offset, timezone)
                      case TimestampType => fieldValues.getElementAsDatetime(Name.getName(requiredField.name)).write(writer, offset, timezone)
                      case ArrayType(StringType, true) =>
                        val elements = fieldValues.numValues()
                        if (elements == 0) {
                          // Array might be empty, resulted in writer not populated at that offset
                          writer.writeStringArray(offset, Seq.empty[String])
                        } else {
                          writer.writeStringArray(offset, (0 until elements).map(index => {
                            val item = fieldValues.getValueAsElement(index)
                            item.getElementAsString(item.name())
                          }))
                        }
                      case _ => throw new IllegalArgumentException(s"Unsupported type [${requiredField.dataType}]")
                    }
                  } else {
                    requiredField.dataType match {
                      case ArrayType(StringType, true) => writer.writeStringArray(offset, Seq.empty[String])
                      case _ => writer.setNullAt(offset)
                    }
                  }
              }
            })
            writer.getRow
          })
        }
      })
    }

    def processBarDataResponse(event: Event): Unit = {
      val security = svcConfig.asInstanceOf[RefDataBarDataConfig].security
      iterator = event.messageIterator.asScala.flatMap(message => {
        if (hasResponseError(message)) None else {

          val tickData = message.getElement(Name.getName("barData"))
          val tickDataArray = tickData.getElement(Name.getName("barTickData"))

          (0 until tickDataArray.numValues).map(i => {

            val tickDataItem = tickDataArray.getValueAsElement(i)

            val writer = new UnsafeRowWriter(schema.length)
            writer.resetRowWriter()

            val fieldDataElements = Map(
              BPipeFields.Response.TIME -> tickDataItem.getElement(Name.getName("time")),
              BPipeFields.Response.OPEN -> tickDataItem.getElement(Name.getName("open")),
              BPipeFields.Response.HIGH -> tickDataItem.getElement(Name.getName("high")),
              BPipeFields.Response.LOW -> tickDataItem.getElement(Name.getName("low")),
              BPipeFields.Response.CLOSE -> tickDataItem.getElement(Name.getName("close")),
              BPipeFields.Response.NUM_EVENTS -> tickDataItem.getElement(Name.getName("numEvents")),
              BPipeFields.Response.VOLUME -> tickDataItem.getElement(Name.getName("volume")),
              BPipeFields.Response.VALUE -> tickDataItem.getElement(Name.getName("value"))
            )

            schema.fields.zipWithIndex.foreach({ case (requiredField, requiredIndex) =>
              requiredField.name match {
                case BPipeFields.Response.SECURITY => writer.write(requiredIndex, UTF8String.fromString(security))
                case _ =>
                  if (fieldDataElements.contains(requiredField.name)) {
                    writer.writeElement(requiredIndex, fieldDataElements(requiredField.name), timezone)
                  } else {
                    requiredField.dataType match {
                      case ArrayType(StringType, true) => writer.writeStringArray(requiredIndex, Seq.empty[String])
                      case _ => writer.setNullAt(requiredIndex)
                    }
                  }
              }
            })
            writer.getRow
          })
        }
      })
    }

    @throws[IllegalStateException]
    def getNextResponseIterator: Boolean = {
      Try {
        session.nextEvent
      } match {
        case Success(eventObj) =>
          if (eventObj.eventType() == Event.EventType.RESPONSE ||
            eventObj.eventType() == Event.EventType.PARTIAL_RESPONSE) {
            serviceName match {
              case INTRADAY_BAR_REQUEST => processBarDataResponse(eventObj)
              case INTRADAY_TICK_REQUEST => processTickDataResponse(eventObj)
              case REFERENCE_DATA_REQUEST => processRefDataResponse(eventObj)
              case HISTORICAL_DATA_REQUEST => processHistoricalDataResponse(eventObj)
            }
            // Indicate if response is only partial
            eventObj.eventType() == Event.EventType.PARTIAL_RESPONSE
          } else {
            throw new IllegalStateException(s"Unsupported event [${eventObj.eventType.toString}]")
          }
        case Failure(exception) =>
          throw new IllegalStateException("Could not process next batch of events", exception)
      }
    }

    override def next(): Boolean = {
      if (iterator.hasNext) true
      else if (isPartialResponse) {
        isPartialResponse = getNextResponseIterator
        next()
      } else false
    }

    override def get(): InternalRow = {
      iterator.next()
    }

    override def close(): Unit = {
      session.stop()
    }
  }

}
