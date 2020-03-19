package batch_layer

import akka.actor.{Actor, ActorSystem, Props}
class analytic {
  //Create a Spark session which connect to Cassandra
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .appName("Lambda architecture - Batch Processing")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  def Analitics: Unit = {

    //Read master_dataset table using DataFrame
    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "messen", "keyspace" -> "monitorsystem"))
      .load()

    //Display some data of master_dataset
    println("Total number of rows: " + df.count())
    println("First 15 rows of the DataFrame: ")
    df.show(15)
  }
}

case object AnalyticProcessing

//Define BatchProcessing actor
class BatchProcessingActor(spark_processor: analytic) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case AnalyticProcessing => {
      println("\nStart hashtag batch processing...")
      spark_processor.Analitics
    }
  }

}