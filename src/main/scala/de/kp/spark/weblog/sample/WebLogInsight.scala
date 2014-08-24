package de.kp.spark.weblog.sample

import de.kp.spark.weblog.LogInsight

object WebLogInsight extends SparkApp {
  
  def main(args:Array[String]) {
    
    val sc = createLocalCtx("WebLogInsight")
    
    val path = "/Work/tmp/web-log/"
    /*
     * Retrieve all pages from the W3C log file with a rating greater than 1
     */
    val pages = LogInsight.fromPages(sc, path + "pages", "select * from pages where rating > 1")  
    pages .foreach(r => println(r))

    /*
     * Retrieve all sessions from the W3C log file with checkout abandonment
     */
    val flows = LogInsight.fromFlows(sc, path + "flows", "select * from flows where flowstatus = 1")  
    flows .foreach(r => println(r))

    sc.stop()
    
  }

}