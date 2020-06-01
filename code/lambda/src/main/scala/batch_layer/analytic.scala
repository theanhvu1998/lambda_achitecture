package batch_layer

import akka.actor.{Actor, ActorRef}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import speed_layer.realTimeProcessing

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
    val MesageDataElectric=MesageData.select(col("type")
      ,col("data")
      ,col("msgid").as("id")
      ,col("time"))

    // transform data
    val MesageDataElectricPaser=MesageDataElectric
      .select(col("type"),explode($"data").as("data")
        ,col("id").as("id")
        ,col("time").as("time"))
    // get data electric
    val ia=MesageDataElectricPaser
      .select(col("id").as("idia")
        ,$"data".getItem("v").alias("g01eleia").cast("Float"))
      .filter($"data".getItem("id").===("g01eleia"))
    val ib=MesageDataElectricPaser
      .select(col("id").as("idib")
        ,$"data".getItem("v").alias("g01eleib").cast("Float"))
      .filter($"data".getItem("id").===("g01eleib"))
    val ic=MesageDataElectricPaser
      .select(col("id").as("idic")
        ,$"data".getItem("v").alias("g01eleic").cast("Float"))
      .filter($"data".getItem("id").===("g01eleic"))
    val vab=MesageDataElectricPaser
      .select(col("id").as("idvab")
        ,$"data".getItem("v").alias("g01elevab").cast("Float"))
      .filter($"data".getItem("id").===("g01elevab"))
    val f=MesageDataElectricPaser
      .select(col("id").as("idf")
        ,$"data".getItem("v").alias("g01elef").cast("Float"))
      .filter($"data".getItem("id").===("g01elef"))
    val s=MesageDataElectricPaser
      .select(col("id").as("ids")
        ,$"data".getItem("v").alias("g01eles").cast("Float"))
      .filter($"data".getItem("id").===("g01eles"))

    // get data environment
    val po=MesageDataElectricPaser
      .select(col("id").as("id")
        ,$"data".getItem("v").alias("g01envpo").cast("Float"))
      .filter($"data".getItem("id").===("g01envpo"))
    val o2=MesageDataElectricPaser
      .select(col("id").as("id")
        ,$"data".getItem("v").alias("g01envo2").cast("Float"))
      .filter($"data".getItem("id").===("g01envo2"))
    val h2s=MesageDataElectricPaser
      .select(col("id").as("id")
        ,$"data".getItem("v").alias("g01envh2s").cast("Float"))
      .filter($"data".getItem("id").===("g01envh2s"))

    // get data operation
    val tb=MesageDataElectricPaser
      .select(col("id").as("tbid")
        ,$"data".getItem("v").alias("g01opetb").cast("Float")
        ,col("time").as("tbtime"))
      .filter($"data".getItem("id").===("g01opetb"))
    val te=MesageDataElectricPaser
      .select(col("id").as("teid")
        ,$"data".getItem("v").alias("g01opete").cast("Float")
        ,col("time").as("tetime"))
      .filter($"data".getItem("id").===("g01opete"))

    // analytic data electric
    val joinedata=ia.join(ib
        ,ia("idia")===ib("idib")
        ,"INNER")
      .join(ic
        ,ia("idia")===ic("idic")
        ,"INNER")
      .join(vab
        ,ia("idia")===vab("idvab")
        ,"INNER")
      .join(f
        ,ia("idia")===f("idf")
        ,"INNER")
      .join(s
        ,ia("idia")===s("ids")
        ,"INNER")

    val joinodata=tb.join(te,te("teid")===tb("tbid")
      ,"INNER")

    val endodata=joinodata.select(col("tbid").as("id")
      ,col("g01opetb")
      ,col("g01opete")
      ,from_unixtime((col("tbtime")/1000).cast("Bigint")).as("time"))
    endodata.show(1000)

    // tranfrom vector data
     //val cassndaradata=joinedata.select(col("idia").as("id"),col("g01elef"),col("g01eleia"),col("g01eleib"),col("g01eleic"),col("g01eles"),col("g01elevab"))

    // Save new data to table
    // cassndaradata.write.format("org.apache.spark.sql.cassandra")
    //  .options(Map("keyspace" -> "monitorsystem", "table" -> "ml"))
    //  .mode(SaveMode.Append)
    //  .save()
    // cassndaradata.show()
  }
}

case object AnalyticProcessing

//Define BatchProcessing actor
class BatchProcessingActor(spark_processor: analytic,realtimeActor: ActorRef) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case AnalyticProcessing => {
      realtimeActor!realTimeProcessing
      println("\nStart batch processing(analytic)...")
      spark_processor.analyticActor
    }
  }
}