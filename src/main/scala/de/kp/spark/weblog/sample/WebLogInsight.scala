package de.kp.spark.weblog.sample

import de.kp.spark.weblog.LogInsight

object WebLogInsight extends SparkApp {
  
  def main(args:Array[String]) {
    
    val sc = createLocalCtx("WebLogInsight")
    
    val path = "/Work/tmp/web-log/"
    val rows = LogInsight.fromPages(sc, path + "pages", "select * from pages where rating > 1")
  
    rows .foreach(r => println(r))
    
    sc.stop()
    
  }

}