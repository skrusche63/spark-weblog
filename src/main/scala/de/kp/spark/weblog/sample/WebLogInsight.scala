package de.kp.spark.weblog.sample

import de.kp.spark.weblog.LogInsight

object WebLogInsight extends SparkApp {
  
  def main(args:Array[String]) {
    
    val sc = createLocalCtx("WebLogInsight")

    val path = "/Work/tmp/web-log/"
    LogInsight.fromPages(sc, path + "pages", "select * from pages")
  
  }

}