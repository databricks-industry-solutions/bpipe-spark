package com.databricks.fsi.bpipe

import com.bloomberglp.blpapi._
import com.databricks.fsi.bpipe.BPipeConfig.{ApiConfig, MktDataConfig, SvcConfig}
import com.databricks.fsi.bpipe.BPipeUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.connector.read.streaming.PartitionOffset
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.time.ZoneId
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object MktDataHandler {

  case class MktDataPartitionPartitionOffset() extends PartitionOffset

  case class MktDataPartitionReaderFactory(
                                            apiConfig: ApiConfig,
                                            schema: StructType,
                                            timezone: ZoneId
                                          ) extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      MktDataPartitionReader(schema, apiConfig, partition.asInstanceOf[SvcConfig], timezone)
    }

  }

  case class MktDataPartitionEventHandler(
                                           schema: StructType,
                                           correlationID: CorrelationID,
                                           securities: List[String],
                                           fields: List[String],
                                           timezone: ZoneId,
                                           queue: BlockingQueue[UnsafeRow]
                                         ) extends EventHandler {

    private val LOGGER = LoggerFactory.getLogger(this.getClass)

    override def processEvent(event: Event, session: Session): Unit = {
      event.eventType match {
        case Event.EventType.SESSION_STATUS =>
          event.messageIterator.asScala.find(message => {
            message.messageType.toString == "SessionStarted"
          }).foreach(_ => {
            session.openServiceAsync(BLP_MKDATA, correlationID)
          })
        case Event.EventType.SERVICE_STATUS =>
          event.messageIterator.asScala.find(message => {
            message.messageType.toString == "ServiceOpened"
          }).foreach(_ => {
            val subscriptionList = new SubscriptionList
            securities.foreach(security => {
              subscriptionList.add(new Subscription(security, fields.asJava))
            })
            session.subscribe(subscriptionList)
          })
        case Event.EventType.SUBSCRIPTION_DATA => handleEvent(event)
        case _ =>
      }
    }

    def handleEvent(event: Event): Unit = {
      event.messageIterator.asScala.flatMap(message => {
        if (hasResponseError(message) || hasSubscriptionFailure(message)) {
          None
        } else {
          // TODO: Find depreciated replacement
          val security = message.topicName
          val writer = new UnsafeRowWriter(schema.length)
          writer.resetRowWriter()

          val fieldValues = fields.filter(field => message.hasElement(Name.getName(field), true)).map(field => {
            val fieldData = message.getElement(Name.getName(field))
            (field, fieldData)
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
      }).foreach(record => {
        queue.add(record)
      })
    }

    def hasResponseError(message: Message): Boolean = {
      if (message.messageType().toString.equals("SubscriptionStarted") &&
        message.hasElement(Name.getName("exceptions"), true)) {
        val exceptions = message.getElement(Name.getName("exceptions"))
        val errorMessages = (0 until exceptions.numValues()).map(i => {
          val exception = exceptions.getValueAsElement(i)
          val reason = exception.getElement(Name.getName("reason"))
          reason.getElementAsString(Name.getName("description"))
        }).mkString("\n")
        LOGGER.error(s"[B-PIPE response error]: $errorMessages")
        true
      } else false
    }

    def hasSubscriptionFailure(message: Message): Boolean = {
      if (message.messageType().toString.equals("SubscriptionFailure") &&
        message.hasElement(Name.getName("reason"), true)) {
        val reason = message.getElement(Name.getName("reason"))
        val description = reason.getElementAsString(Name.getName("description"))
        LOGGER.error(s"[B-PIPE subscription error]: $description")
        true
      } else false
    }
  }

  case class MktDataPartitionReader(
                                     schema: StructType,
                                     apiConfig: ApiConfig,
                                     svcConfig: SvcConfig,
                                     timezone: ZoneId
                                   ) extends PartitionReader[InternalRow] {

    private val LOGGER = LoggerFactory.getLogger(this.getClass)
    private val queue: BlockingQueue[UnsafeRow] = new LinkedBlockingDeque[UnsafeRow]()
    private val timeout = 10000 // Fails  if communication from multiple threads cannot be fully established after 10sec
    private val mktConfig = svcConfig.asInstanceOf[MktDataConfig]
    private var session: Session = _
    private var event: UnsafeRow = _

    Try {

      // Instantiate new session
      LOGGER.info("Starting new B-PIPE session asynchronously")
      val sessionOptions = new SessionOptions
      sessionOptions.setServerHost(apiConfig.serviceHost)
      sessionOptions.setServerPort(apiConfig.servicePort)

      // Start session asynchronously
      val eventHandler = MktDataPartitionEventHandler(
        schema,
        new CorrelationID(apiConfig.correlationId),
        mktConfig.securities,
        mktConfig.fields,
        timezone,
        queue
      )

      session = new Session(sessionOptions, eventHandler)
      session.startAsync()

      // We have to find a way to block thread until we're done communicating with server and start receiving data
      LOGGER.info("Waiting for session to be established")
      val now = System.currentTimeMillis()
      while (queue.isEmpty) {
        if (System.currentTimeMillis() - now > timeout)
          throw new IllegalStateException("Could not establish connection to B-PIPE service in time")
        Thread.sleep(1000)
      }

      LOGGER.info("Session established, streaming events")

    } match {
      case Success(_) =>
      case Failure(exception) =>
        LOGGER.error(s"[B-PIPE connection error]: ${exception.getMessage}")
        throw new IllegalStateException("[B-PIPE connection error]", exception)
    }

    override def next(): Boolean = {
      // get method needs to pull same results until next() is called.
      if (queue.isEmpty) false else {
        event = queue.poll()
        true
      }
    }

    override def get(): InternalRow = {
      event
    }

    override def close(): Unit = {
      val subscriptionList = new SubscriptionList
      mktConfig.securities.foreach(security => {
        subscriptionList.add(new Subscription(security, mktConfig.fields.asJava))
      })
      session.unsubscribe(subscriptionList)
      session.stop()
    }

  }

}
