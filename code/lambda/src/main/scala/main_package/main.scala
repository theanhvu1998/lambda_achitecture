package main_package

import akka.actor.{ActorSystem, Props}
import batch_layer.{AnalyticProcessing, BatchProcessingActor, data_analytic}
import config.AppConfiguration
import speed_layer.{realTimeProcessing, realTimeProcessingActor, real_time_data}

import scala.concurrent.duration._


object main {
  def main(args: Array[String]): Unit = {
    // Run Batch processing and realtime processing
    runProcessing()
  }

  def runProcessing()={
    //Creating an ActorSystem
    val actorSystem = ActorSystem("ActorSystem");

    //Create batch actor
    val realtimeActor = actorSystem.actorOf(Props(new realTimeProcessingActor( new real_time_data)))
    val batchActor = actorSystem.actorOf(Props(new BatchProcessingActor(new data_analytic)))

    //Using akka scheduler to run the batch processing periodically
    import actorSystem.dispatcher
    val initialDelay = 100 milliseconds
    val batchInterval=AppConfiguration.batchInterval

    //running batch processing after each 30 mins
    actorSystem.scheduler.schedule(initialDelay,batchInterval,batchActor,AnalyticProcessing)
    realtimeActor!realTimeProcessing
  }
}
