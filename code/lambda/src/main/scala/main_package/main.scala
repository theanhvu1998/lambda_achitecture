package main_package

import akka.actor.{Actor, ActorSystem, Props}
import batch_layer.{AnalyticProcessing, BatchProcessingActor, analytic}
import config.AppConfiguration


import scala.concurrent.duration._
import org.apache.spark.sql.functions.lower


object main {
  def main(args: Array[String]): Unit = {
    //Creating an ActorSystem
    val actorSystem = ActorSystem("ActorSystem");

    //Create batch actor
    val batchActor = actorSystem.actorOf(Props(new BatchProcessingActor(new analytic)))
    //Using akka scheduler to run the batch processing periodically
    import actorSystem.dispatcher
    val initialDelay = 100 milliseconds
    val batchInterval=AppConfiguration.batchInterval //running batch processing after each 30 mins

    actorSystem.scheduler.schedule(initialDelay,batchInterval,batchActor,AnalyticProcessing)
  }
}
