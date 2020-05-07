package date_collect

import java.text.SimpleDateFormat

import java.sql.Date
import net.liftweb.json.DefaultFormats
import org.apache.spark.sql.functions._
import scala.collection.mutable._
import net.liftweb.json.Serialization.write
import collection.JavaConverters.setAsJavaSetConverter

case class datatbale(date: Date)

class datecollect {
  val dateFmt = "yyyy-MM-dd"
  //Create a Spark session which connect to Cassandra
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .appName("Lambda architecture - date collect")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  def collectdata: Unit ={
    val dateFmt = "yyyy-MM-dd"
    var setdata = scala.collection.mutable.Set[datatbale]();
    val date = "2020-04-01"
    val formate = new SimpleDateFormat(dateFmt).parse(date)
    var aDate = new java.sql.Date(formate.getTime());
    for(date <- 1 to 100){
      aDate = new Date(aDate.getTime + 24 * 60 * 60 * 1000)
      val ee = datatbale(aDate)
      setdata.add(ee)
    }

    val df = setdata.toList.toDF()
    val dfshow = df.sort(asc("date"))
    dfshow.show(300)
  }
}
