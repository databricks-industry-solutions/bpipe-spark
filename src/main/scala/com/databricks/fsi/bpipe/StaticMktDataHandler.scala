package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi.SessionOptions.ServerAddress
import com.bloomberglp.blpapi._
import com.databricks.fsi.bpipe.BPipeConfig._
import com.databricks.fsi.bpipe.BPipeUtils._
import org.apache.commons.io.IOUtils
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

object StaticMktDataHandler {

  case class StaticMktDataPartitionReaderFactory(
                                                  serviceName: String,
                                                  apiConfig: BpipeApiConfig,
                                                  schema: StructType,
                                                  timezone: ZoneId
                                                ) extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      StaticMktDataPartitionReader(serviceName, schema, apiConfig, partition.asInstanceOf[SvcConfig], timezone)
    }
  }

  case class StaticMktDataPartitionReader(
                                           serviceName: String,
                                           schema: StructType,
                                           apiConfig: BpipeApiConfig,
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
      val serverAddresses = apiConfig.serverAddresses.map { host =>
        new ServerAddress(host, apiConfig.serverPort)
      }
      sessionOptions.setServerAddresses(serverAddresses)
      sessionOptions.setTlsOptions(TlsOptions.createFromBlobs(
        IOUtils.toByteArray(this.getClass.getResourceAsStream(apiConfig.tlsPrivateKeyPath)),
        apiConfig.tlsPrivateKeyPassword.toCharArray,
        IOUtils.toByteArray(this.getClass.getResourceAsStream(apiConfig.tlsCertificatePath))
      ))
      val authOptions = new AuthOptions(new AuthApplication(apiConfig.authApplicationName))
      sessionOptions.setSessionIdentityOptions(authOptions)
      session = new Session(sessionOptions)
      session.start

      // Open service to tick data API
      LOGGER.info(s"Opening service [$BLP_STATICMKTDATA]")
      session.openService(BLP_STATICMKTDATA)
      val service = session.getService(BLP_STATICMKTDATA)

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
        if (fieldExceptions.numValues() > 0) {
          val messages = (0 until fieldExceptions.numValues()).map(i => {
            val fieldError = fieldExceptions.getValueAsElement(i)
            val errorInfo = fieldError.getElement(Name.getName("errorInfo"))
            errorInfo.getElementAsString(Name.getName("message"))
          }).mkString("\n")
          LOGGER.error(s"[B-PIPE fields error]: $messages")
          true
        } else {
          false
        }
      } else false
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

    @throws[IllegalStateException]
    def getNextResponseIterator: Boolean = {
      Try {
        session.nextEvent
      } match {
        case Success(eventObj) =>
          LOGGER.info("Received BPipe Event [{}]", eventObj.eventType())
          val hasNext = if (eventObj.eventType() == Event.EventType.RESPONSE ||
            eventObj.eventType() == Event.EventType.PARTIAL_RESPONSE) {
            serviceName match {
              case REFERENCE_DATA_REQUEST => processRefDataResponse(eventObj)
            }
            // Indicate if response is only partial
            eventObj.eventType() == Event.EventType.PARTIAL_RESPONSE
          } else {
            true
          }
          hasNext
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
