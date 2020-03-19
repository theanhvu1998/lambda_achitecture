package config

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

object AppConfiguration {

  val config = ConfigFactory.load()
  // Kafka Config
  val kafkaTopic=config.getString("kafka.topic")
  val kafkaKeywords = config.getStringList("kafka.keywords").toList

  // Batch processing config
  // Convert Duration to Finite Duration
  val batchInterval=Duration.fromNanos(config.getDuration("batchProcessing.batchInterval").toNanos)
}
