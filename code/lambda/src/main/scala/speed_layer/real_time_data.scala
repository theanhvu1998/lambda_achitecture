package speed_layer

import akka.actor.Actor
import config.AppConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, desc, explode, from_json, lower, unix_timestamp}

class real_time_data {
  //Define a Spark session
  val spark=SparkSession.builder().appName("Lambda Architecture - Speed layer")
    .master("local")
    .getOrCreate()

  //Set the Log file level
  spark.sparkContext.setLogLevel("WARN")

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Define Schema of received tweet streem
  val DataScheme
  = StructType(
    List(
      StructField("rpi", StringType, true),
      StructField("type", StringType, true),
      StructField("msgid", StringType, true),
      StructField("status", StringType,true)
    )
  )

  def realtimeAactor: Unit = {
    //Subscribe Spark the defined Kafka topic
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", AppConfiguration.kafkaTopicStatus)
      .option("failOnDataLoss",false)
      .load()

    //Reading the streaming json data with its schema
    val messenger = df.selectExpr("CAST(value AS STRING) as jsonData")
      .select(from_json($"jsonData"
        , schema = DataScheme).as("status"))
      .select("status.*")

    val messengerpaser = messenger.select(col("status")
        ,col("rpi")
        ,col("msgid")
        ,col("type"))
        .withColumn("time",(unix_timestamp()*1000))

    messengerpaser.selectExpr("CAST(msgid AS STRING) AS key"
        , "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", AppConfiguration.kafkaTopicStatusAnalitic)
      .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")
      .start()
  }
}
case object realTimeProcessing

//Define BatchProcessing actor
class realTimeProcessingActor(spark_processor: real_time_data) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case realTimeProcessing => {
      println("\nStart speed processing...")
      spark_processor.realtimeAactor
    }
  }
}