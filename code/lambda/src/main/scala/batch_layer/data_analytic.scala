package batch_layer

import akka.actor.{Actor, ActorRef}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import speed_layer.realTimeProcessing

class data_analytic {
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

  def analyticActor: Unit = {

    //Read master_dataset table using DataFrame
    val MesageData = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "messenger", "keyspace" -> "monitorsystem"))
      .load()
    /**
     * Get data form cassandra
     * Transform data and analytic
     *  Save data to cassandra
     * */
    // val uuid = udf(() => java.util.UUID.randomUUID().toString)
    //  val dfa=dff.withColumn("uuid", uuid())

    // get all data
    val MesageDataElectric=MesageData.select(col("msgid").as("id")
      ,col("sessionid")
      ,col("rpi")
      ,col("type")
      ,col("data")
      ,col("time")).filter(MesageData("time").cast("Float") > (unix_timestamp()*1000-2000000))

    // transform data
    val MesageDataElectricPaser=MesageDataElectric
      .select(col("type"),explode($"data").as("data")
        ,col("sessionid")
        ,col("rpi")
        ,col("id").as("id")
        ,col("time").as("time"))
    // get data operation
    val tb=MesageDataElectricPaser
      .select(col("id").as("tbid")
        ,col("sessionid")
        ,col("rpi")
        ,$"data".getItem("v").alias("g01opetb").cast("Float")
        ,col("time").as("tbtime"))
      .filter($"data".getItem("id").===("g01opetb"))
    val te=MesageDataElectricPaser
      .select(col("id").as("teid")
        ,$"data".getItem("v").alias("g01opete").cast("Float")
        ,col("time").as("tetime"))
      .filter($"data".getItem("id").===("g01opete"))

    val joinodata=tb.join(te,te("teid")===tb("tbid")
      ,"INNER")

    val operationdata=joinodata.select(col("tbid").as("id")
      ,col("sessionid")
      ,col("rpi")
      ,col("g01opetb")
      ,col("g01opete")
      ,col("tbtime").cast("Bigint").as("time"))
    val operationdatafilter=operationdata.filter(operationdata("g01opete") =!=(0))
    val operationdataanalytic=operationdatafilter.select(((col("g01opete") - col("g01opetb"))/1000/3600).as("duration")
      ,col("id")
      ,col("sessionid")
      ,col("rpi")
      ,col("time"))
    operationdata.show(10000)
    operationdataanalytic.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "analyticoperation")
      .save()
     //tranfrom vector data
     //val cassndaradata=joinedata.select(col("idia").as("id"),col("g01elef"),col("g01eleia"),col("g01eleib"),col("g01eleic"),col("g01eles"),col("g01elevab"))

    // Save new data to table
    operationdataanalytic.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "monitorsystem", "table" -> "analyticoperation"))
      .mode(SaveMode.Append)
      .save()
  }
}

case object AnalyticProcessing

//Define BatchProcessing actor
class BatchProcessingActor(spark_processor: data_analytic) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case AnalyticProcessing => {
      println("\nStart batch processing(analytic)...")
      spark_processor.analyticActor
    }
  }
}