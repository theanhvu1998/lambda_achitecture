package config

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

object AppConfiguration {
  val config = ConfigFactory.load()
  // Kafka Config
  val kafkaTopicStatus=config.getString("kafka.topicStatus")
  val kafkaTopicStatusAnalitic=config.getString("kafka.topicStatusAnalytic")

  // Batch processing config
  // Convert Duration to Finite Duration
  val batchInterval=Duration.fromNanos(config.getDuration("batchProcessing.batchInterval").toNanos)
}
