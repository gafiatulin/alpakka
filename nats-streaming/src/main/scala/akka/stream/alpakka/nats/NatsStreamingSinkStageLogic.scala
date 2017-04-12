package akka.stream.alpakka.nats

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape}
import io.nats.client._
import io.nats.streaming.{AckHandler, StreamingConnection}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class NatsStreamingSimpleSinkStage(settings: PublishingSettings) extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage[Array[Byte]]], Future[Done]]{
  val in: Inlet[OutgoingMessage[Array[Byte]]] = Inlet("NatsStreamingSimpleSink.in")
  val shape: SinkShape[OutgoingMessage[Array[Byte]]] = SinkShape(in)
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new NatsStreamingSimpleSinkStageLogic(settings, promise, shape, in)
    (logic, promise.future)
  }
}

class NatsStreamingSinkWithCompletionStage(settings: PublishingSettings) extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessageWithCompletion[Array[Byte]]], Future[Done]]{
  val in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]] = Inlet("NatsStreamingSinkWithComplete.in")
  val shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]] = SinkShape(in)
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new NatsStreamingSinkWithCompletionStageLogic(settings, promise, shape, in)
    (logic, promise.future)
  }
}

private[nats] abstract class NatsStreamingSinkStageLogic[T <: NatsStreamingOutgoing[Array[Byte]]](
  settings: PublishingSettings,
  promise: Promise[Done],
  shape: SinkShape[T],
  in: Inlet[T]
) extends GraphStageLogic(shape) with StageLogging{
  protected val successCallback: AsyncCallback[String] = getAsyncCallback(handleSuccess)
  protected val failureCallback: AsyncCallback[Exception] = getAsyncCallback(handleFailure)
  private var connection: StreamingConnection = _
  def ah(m: T): AckHandler

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
        pull(in)
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

  def handleFailure(ex: Exception): Unit = {
    log.error("Caught Exception. Failing stage...", ex)
    failStage(ex)
  }

  def handleSuccess(nuid: String): Unit = {
    log.debug("Successfully pushed message with id {}", nuid)
    if(settings.parallel) () else pull(in)
  }

  setHandler(in, new InHandler {
    override def onPush(): Unit = {
      val m = grab(in)
      connection.publish(settings.subject, m.data, ah(m))
      if(settings.parallel) pull(in) else ()
    }
  })


}

private[nats] class NatsStreamingSimpleSinkStageLogic(
  settings: PublishingSettings,
  promise: Promise[Done],
  shape: SinkShape[OutgoingMessage[Array[Byte]]],
  in: Inlet[OutgoingMessage[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(settings, promise, shape, in){
  def ah(m: OutgoingMessage[Array[Byte]]): AckHandler = new AckHandler {
    def onAck(nuid: String, ex: Exception): Unit = if(Option(ex).isDefined) failureCallback.invoke(ex) else successCallback.invoke(nuid)
  }
}

private[nats] class NatsStreamingSinkWithCompletionStageLogic(
                                                             settings: PublishingSettings,
                                                             promise: Promise[Done],
                                                             shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]],
                                                             in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(settings, promise, shape, in){
  def ah(m: OutgoingMessageWithCompletion[Array[Byte]]): AckHandler = new AckHandler {
    def onAck(nuid: String, ex: Exception): Unit = if(Option(ex).isDefined){
      m.promise.tryFailure(ex)
      failureCallback.invoke(ex)
    } else {
      m.promise.trySuccess(Done)
      successCallback.invoke(nuid)
    }
  }
}