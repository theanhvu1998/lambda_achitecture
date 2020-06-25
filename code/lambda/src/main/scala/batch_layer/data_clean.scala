package batch_layer

import akka.actor.Actor
import config.AppConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType, StructField, StructType}
import speed_layer.real_time_data

class data_clean {
  //Define a Spark session
  val spark=SparkSession.builder().appName("Lambda Architecture - Speed layer")
    .master("local")
    .getOrCreate()

  //Set the Log file level
  spark.sparkContext.setLogLevel("WARN")

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Define Schema of received tweet streem
  val twitterDataScheme
  = StructType(
    List(
      StructField("rpi", StringType, true),
      StructField("time", LongType, true),
      StructField("type", StringType, true),
      StructField("msgid", StringType, true),
      StructField("sessionid", StringType, true),
      StructField("data", ArrayType(MapType(StringType,StringType,true),true),true)
    )
  )

  def cleanDataActor: Unit = {
    //Subscribe Spark the defined Kafka topic
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", AppConfiguration.kafkaTopicStatus)
      .load()

    //Reading the streaming json data with its schema
    val messenger = df.selectExpr("CAST(value AS STRING) as jsonData")
      .select(from_json($"jsonData"
        , schema = twitterDataScheme).as("data"))
      .select("data.*")
    messenger
    messenger.selectExpr("CAST(msgid AS STRING) AS key"
      , "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test")
      .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")
      .start()
  }
}

case object cleanDataprocessing

//Define BatchProcessing actor
class realTimeProcessingActor(spark_processor: data_clean) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case cleanDataprocessing => {
      println("\nStart speed processing...")
      spark_processor.cleanDataActor
    }
  }
}