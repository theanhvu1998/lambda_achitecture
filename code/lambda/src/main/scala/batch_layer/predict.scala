package batch_layer

import akka.actor.{Actor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

class predict {
  //Create a Spark session which connect to Cassandra
  val spark=SparkSession.builder().appName("Lambda Architecture - batch layer - predict")
    .master("local")
    .getOrCreate()

  //Implicit methods available in Scala for converting common Scala objects into DataFrames
  import spark.implicits._

  //Get Spark Context from Spark session
  val sparkContext = spark.sparkContext

  //Set the Log file level
  sparkContext.setLogLevel("WARN")

  def predictActor: Unit = {

    //Read master_dataset table using DataFrame
    val featureData = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "ml", "keyspace" -> "monitorsystem"))
      .load()
    /**
     * Get data form cassandra
     * Transform data and analytic
     *  Save data to cassandra
     * */

    featureData.show()

    // val uuid = udf(() => java.util.UUID.randomUUID().toString)
    //  val dfa=dff.withColumn("uuid", uuid())

    // tranform data to vector
       val endedata=new VectorAssembler().
        setInputCols(Array( "g01eleia","g01eleib", "g01eleic","g01elevab","g01elef","g01eles")).
        setOutputCol("features").
       transform(featureData)

    // tranfrom vector data
    // val cassndaradata=endedata.select(col("idia").as("id"),col("g01elef"),col("g01eleia"),col("g01eleib"),col("g01eleic"),col("g01eles"),col("g01elevab"))

    // Save new data to table
    // cassndaradata.write.format("org.apache.spark.sql.cassandra")
    //  .options(Map("keyspace" -> "monitorsystem", "table" -> "ml"))
    //  .mode(SaveMode.Append)
    //  .save()
    // cassndaradata.show()
    //joinedata.selectExpr("CAST(idia AS STRING) AS key", "to_json(struct(*)) AS value")
    // .write
    // .format("kafka")
    // .option("kafka.bootstrap.servers", "localhost:9092")
    // .option("topic", "test")
    // .save()
  }
}

case object predictProcessing

//Define BatchProcessing actor
class predictProcessingActor(spark_processor: predict) extends Actor{

  //Implement receive method
  def receive = {
    //Start hashtag batch processing
    case predictProcessing => {
      println("\nStart batch processing(analytic)...")
      spark_processor.predictActor
    }
  }
}