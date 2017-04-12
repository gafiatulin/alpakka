package akka.stream.alpakka.nats

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import io.nats.client._
import io.nats.streaming.{Message, MessageHandler, StreamingConnection, SubscriptionOptions}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class NatsStreamingSimpleSourceStage(settings: SimpleSubscriptionSettings)
  extends GraphStage[SourceShape[IncomingMessage[Array[Byte]]]]{
  val out: Outlet[IncomingMessage[Array[Byte]]] = Outlet("NatsStreamingSimpleSource.out")
  val shape: SourceShape[IncomingMessage[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new NatsStreamingSimpleSourceStageLogic(settings, shape, out)
}

class NatsStreamingSourceWithAckStage(settings: SubscriptionWithAckSettings)
  extends GraphStage[SourceShape[IncomingMessageWithAck[Array[Byte]]]]
{
  require(settings.manualAckTimeout.compareTo(settings.autoRequeueTimeout) <= 0)
  val out: Outlet[IncomingMessageWithAck[Array[Byte]]] = Outlet("NatsStreamingSourceWithAck.out")
  val shape: SourceShape[IncomingMessageWithAck[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new NatsStreamingSourceWithAckStageLogic(settings, shape, out)
}

private[nats] abstract class NatsStreamingSourceStageLogic[T1 <: NatsStreamingSubscriptionSettings, T2 <: NatsStreamingIncoming[Array[Byte]]](
  settings: T1,
  shape: SourceShape[T2],
  out: Outlet[T2]
) extends GraphStageLogic(shape) with StageLogging{
  private val queue = new LinkedBlockingQueue[T2](settings.maxBufferSize)
  private val successCallback: AsyncCallback[T2] = getAsyncCallback(handleSuccess)
  private val failureCallback: AsyncCallback[Exception] = getAsyncCallback(handleFailure)
  private var connection: StreamingConnection = _
  val optionBuilder: SubscriptionOptions.Builder = {
    val b = new SubscriptionOptions.Builder()
      .maxInFlight(settings.subMaxInFlight)
    settings.durableSubscriptionName.map(b.durableName).getOrElse(b)
  }
  val messageCallback: MessageHandler = new MessageHandler {
    def onMessage(msg: Message): Unit = if(queue.size() < settings.maxBufferSize) callbackLogic(queue, msg) else ()
  }

  override def preStart(): Unit = {
    Try(settings.cp.connection) match {
      case Success(c) =>
        connection = c
        val natsConnection = connection.getNatsConnection
        natsConnection.setClosedCallback(new ClosedCallback {
          def onClose(event: ConnectionEvent): Unit = log.debug("Connection closed {}", event)
        })
        natsConnection.setDisconnectedCallback(new DisconnectedCallback{
          def onDisconnect(event: ConnectionEvent): Unit = log.debug("Disconnected {}", event)
        })
        natsConnection.setReconnectedCallback(new ReconnectedCallback{
          def onReconnect(event: ConnectionEvent): Unit = log.debug("Reconnected {}", event)
        })
        natsConnection.setExceptionHandler(new ExceptionHandler{
          def onException(ex: NATSException): Unit = failureCallback.invoke(ex)
        })
        connection.subscribe(settings.subject, settings.subscriptionQueue, messageCallback, options)
      case Failure(e: Exception) =>
        failureCallback.invoke(e)
      case Failure(t) =>
        failureCallback.invoke(new Exception(t.getMessage))
    }
  }
  override def postStop(): Unit = {
    Try(connection.close())
    ()
  }

  def handleSuccess(v: T2): Unit = push(out, v)

  def handleFailure(ex: Exception): Unit = {
    log.error("Caught Exception. Failing stage...", ex)
    failStage(ex)
  }

  def options: SubscriptionOptions
  def callbackLogic(queue: LinkedBlockingQueue[T2], msg: Message): Unit

  setHandler(out, new OutHandler {
    override def onPull(): Unit = if (queue.isEmpty){
      Future(queue.poll(Long.MaxValue, TimeUnit.DAYS))(materializer.executionContext)
        .foreach(successCallback.invoke)(materializer.executionContext)
    } else {
      push(out, queue.poll())
    }
  })
}

private[nats] class NatsStreamingSimpleSourceStageLogic(
  settings: SimpleSubscriptionSettings,
  shape: SourceShape[IncomingMessage[Array[Byte]]],
  out: Outlet[IncomingMessage[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(settings, shape, out){

  override def options: SubscriptionOptions = optionBuilder.build()

  override def callbackLogic(queue: LinkedBlockingQueue[IncomingMessage[Array[Byte]]], msg: Message): Unit = {
    val m = IncomingMessage(msg.getData)
    queue.put(m)
  }
}

private[nats] class NatsStreamingSourceWithAckStageLogic(
  settings: SubscriptionWithAckSettings,
  shape: SourceShape[IncomingMessageWithAck[Array[Byte]]],
  out: Outlet[IncomingMessageWithAck[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(settings, shape, out){

  override def options: SubscriptionOptions = optionBuilder.ackWait(settings.autoRequeueTimeout).manualAcks().build()

  override def callbackLogic(queue: LinkedBlockingQueue[IncomingMessageWithAck[Array[Byte]]], msg: Message): Unit = {
    val m = IncomingMessageWithAck(msg.getData, Promise[Done])
    queue.put(m)
    m.promise.future.onComplete{
      case Success(_) => msg.ack()
      case _ => ()
    }(materializer.executionContext)
    materializer.scheduleOnce(FiniteDuration(settings.manualAckTimeout.toNanos, TimeUnit.NANOSECONDS), new Runnable {
      def run(): Unit = {
        m.promise.tryFailure(new Exception(s"Didn't process message during ${settings.manualAckTimeout}"))
        ()
      }
    })
  }
}