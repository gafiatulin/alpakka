package akka.stream.alpakka.nats

import java.time.Duration

import com.typesafe.config.Config

import scala.util.Try

final case class NatsStreamingConnectionSettings(
  clusterId: String,
  clientId: String,
  url: String,
  connectionTimeout: Option[Duration],
  publishAckTimeout: Option[Duration],
  publishMaxInFlight: Option[Int],
  discoverPrefix: Option[String]
)

case object NatsStreamingConnectionSettings{
  private def url(host: String, port: Int): String = "nats://" + host + ":" + port.toString
  def fromConfig(config: Config): NatsStreamingConnectionSettings =
    NatsStreamingConnectionSettings(
      config.getString("clusterId"),
      config.getString("clientId"),
      Try(config.getString("url")).getOrElse(url(config.getString("host"), config.getInt("port"))),
      Try(config.getDuration("connectionTimeout")).toOption,
      Try(config.getDuration("pubAckTimeout")).toOption,
      Try(config.getInt("pubMaxInFlight")).toOption,
      Try(config.getString("discoverPrefix")).toOption
    )
}

sealed trait NatsStreamingSubscriptionSettings{
  def cp: StreamingConnectionProvider
  def subject: String
  def subscriptionQueue: String
  def durableSubscriptionName: Option[String]
  def subMaxInFlight: Int
  def maxBufferSize: Int
}

object NatsStreamingSubscriptionSettings{
  val defaultBufferSize = 100
}

final case class SimpleSubscriptionSettings(
  cp: StreamingConnectionProvider,
  subject: String,
  subscriptionQueue: String,
  durableSubscriptionName: Option[String],
  subMaxInFlight: Int,
  maxBufferSize: Int
) extends NatsStreamingSubscriptionSettings

case object SimpleSubscriptionSettings{
  def fromConfig(config: Config): SimpleSubscriptionSettings =
    SimpleSubscriptionSettings(
      NatsStreamingConnectionBuilder.fromConfig(config),
      config.getString("subject"),
      config.getString("subscriptionQueue"),
      Try(config.getString("durableSubscriptionName")).toOption,
      config.getInt("subscriptionMaxInFlight"),
      Try(config.getInt("maxBufferSize")).getOrElse(NatsStreamingSubscriptionSettings.defaultBufferSize)
    )
}

final case class SubscriptionWithAckSettings(
  cp: StreamingConnectionProvider,
  subject: String,
  subscriptionQueue: String,
  durableSubscriptionName: Option[String],
  subMaxInFlight: Int,
  manualAckTimeout: Duration,
  autoRequeueTimeout: Duration,
  maxBufferSize: Int
) extends NatsStreamingSubscriptionSettings

case object SubscriptionWithAckSettings{
  def fromConfig(config: Config): SubscriptionWithAckSettings = {
    val simple = SimpleSubscriptionSettings.fromConfig(config)
    SubscriptionWithAckSettings(
      simple.cp,
      simple.subject,
      simple.subscriptionQueue,
      simple.durableSubscriptionName,
      simple.subMaxInFlight,
      config.getDuration("manualAckTimeout"),
      config.getDuration("autoRequeueTimeout"),
      simple.maxBufferSize
    )
  }
}

final case class PublishingSettings(
  cp: StreamingConnectionProvider,
  subject: String,
  parallel: Boolean
)

case object PublishingSettings{
  def fromConfig(config: Config): PublishingSettings =
  PublishingSettings(
    NatsStreamingConnectionBuilder.fromConfig(config),
    config.getString("subject"),
    config.getBoolean("parallel")
  )
}