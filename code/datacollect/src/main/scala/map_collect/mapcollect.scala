package map_collect

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


class mapcollect {

  //Create a Spark session which connect to Cassandra
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .appName("Lambda architecture - map collect")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")
  def collectdata: Unit ={
    //Read master_dataset table using DataFrame
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val pathcurrent=System.getProperty("user.dir")
    //val path = "vietnamLow.json"
    val path = pathcurrent+"/src/main/resources/vietnamLow.json"
    val mapDF = spark.read.json(path)
    val mapdata = mapDF.select($"properties".getItem("id").as("amcahrtid"),$"properties".getItem("name").as("name"),$"properties".getItem("NAME_ENG").as("name_eng"),$"properties".getItem("TYPE").as("type"),$"properties".getItem("TYPE_ENG").as("type_eng"))
    val enddata = mapdata.withColumn("id", monotonically_increasing_id())

   // enddata.write.format("org.apache.spark.sql.cassandra")
    // .options(Map("keyspace"->"monitorsystem","table"->"city"))
    // .mode(SaveMode.Append)
     //.save()
    println("data collect.......")
    println("map data:")

    enddata.show(300)
  }
}
