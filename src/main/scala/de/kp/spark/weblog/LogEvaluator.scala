package de.kp.spark.weblog
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Weblog project
* (https://github.com/skrusche63/spark-weblog).
* 
* Spark-Weblog is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Weblog is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Weblog. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object LogEvaluator {

  /**
   * This method determines the time spent on a certain page in seconds and assigns 
   * a specific rating from a predefined time rating (see configuration)
   * 
   * Input: session = (sessionid,timestamp,userid,pageurl,visittime,referrer)
   */
  def timeSpentAndRating(source:RDD[(String,Long,String,String,String,String)]):RDD[String] = {

    val sc = source.context
    val ratings = sc.broadcast(Configuration.ratings)
    
    /* Group source by sessionid */
    val dataset = source.groupBy(group => group._1)
    dataset.flatMap(valu => {
      
      /* Sort session data by timestamp */
      val data = valu._2.toList.sortBy(_._2)
      
      /* Compute page time = difference between two session events */ 
      var sessid:String = null
      var userid:String = null
      
      var lasturl:String  = null
      var referrer:String = null
      
      var starttime:Long = 0
      var endtime:Long   = 0
      
      var visittime:String = null
      
      var first = true
      
      val output = ArrayBuffer.empty[String]

      for (entry <- data) {
        
        if (first) {     
          
          var (sessid,starttime,userid,lasturl,visittime,referrer) = entry
          
          endtime = starttime
          first = false
          
        } else {     

          val timespent  = (entry._2 - endtime) / 1000

          /* Compute rating from pagetime */
          var rating = 0
          breakable {for (entry <- ratings.value) {
            
            if (timespent < entry._1) {
              rating = entry._2
              break
            }
            
          }} 
          
          rating = if (rating == 0) ratings.value.last._2 else rating
          
          val out = sessid + "|" + userid + "|" + starttime + "|" + lasturl + "|" + visittime + "|" + referrer + "|" + timespent + "|" + rating
          output += out

          endtime = entry._2
          lasturl = entry._4
        
          visittime = entry._5
      
        }
      
      }
      
      /* Last page */
      val rating    = 0
      val timespent = 0
        
      val out = sessid + "|" + userid + "|" + starttime + "|" + lasturl + "|" + visittime + "|" + referrer + "|" + timespent + "|" + rating
      output += out

      output
      
    })
     
  }
 
}