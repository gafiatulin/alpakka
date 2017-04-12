package akka.stream.alpakka.nats

import akka.Done

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

sealed trait NatsStreamingMessage[T]{
  def data: T
  def transform[T2](f: T => T2): NatsStreamingMessage[T2]
}

sealed trait NatsStreamingIncoming[T] extends NatsStreamingMessage[T]
sealed trait NatsStreamingOutgoing[T] extends NatsStreamingMessage[T]

case class IncomingMessage[T](data: T) extends NatsStreamingIncoming[T]{
  def transform[T2](f: (T) => T2): IncomingMessage[T2] = IncomingMessage(f(data))
}

case class IncomingMessageWithAck[T](data: T) extends NatsStreamingIncoming[T]{
  private[nats] val promise = Promise[Done]
  def transform[T2](f: (T) => T2): IncomingMessageWithAck[T2] = IncomingMessageWithAck(f(data), promise)
  def ack: Try[T] = if(promise.trySuccess(Done)) Success(data) else Failure(new Exception("Already completed"))
}

object IncomingMessageWithAck{
  private[nats] def apply[T](data: T, p: Promise[Done]) = new IncomingMessageWithAck(data){
    override val promise: Promise[Done] = p
  }
}

case class OutgoingMessage[T](data: T) extends NatsStreamingOutgoing[T]{
  def transform[T2](f: (T) => T2): NatsStreamingOutgoing[T2] = OutgoingMessage(f(data))
}

case class OutgoingMessageWithCompletion[T](data: T) extends NatsStreamingOutgoing[T]{
  private[nats] val promise = Promise[Done]
  def transform[T2](f: (T) => T2): OutgoingMessageWithCompletion[T2] = OutgoingMessageWithCompletion(f(data), promise)
  def completion(implicit ec: ExecutionContext): Future[Done] = promise.future
}

object OutgoingMessageWithCompletion{
  private[nats] def apply[T](data: T, p: Promise[Done]) = new OutgoingMessageWithCompletion(data){
    override val promise: Promise[Done] = p
  }
}

