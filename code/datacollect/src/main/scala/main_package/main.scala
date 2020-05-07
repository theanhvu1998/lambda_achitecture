package main_package

import map_collect.mapcollect
import date_collect.datecollect

object main {
  def main(args: Array[String]): Unit = {
    runProcessing()
  }

  def runProcessing()={
    //val mapdata=new mapcollect
   // val newdata=mapdata.collectdata
    val datedata=new datecollect
    val newdata=datedata.collectdata
  }
}
