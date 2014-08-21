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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import de.kp.spark.weblog.Configuration._

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.util.control.Breaks._

object LogExtractor {

  private val settings  = Configuration.config
  private val fieldspec = Configuration.fieldspec
    
  /**
   * This is the basic method to extract valuable information from the entries of the W3 log file
   * 
   * 2012-04-27  00:07:40  __RequestVerificationToken_Lw__=3GQ426510U4H;+.ASPXAUTH=DJ4XNH6EMMW5CCC5  /product/N19C4MX1 http://www.healthyshopping.com/product/T0YJZ1QH
   * 2012-04-27  00:08:21  __RequestVerificationToken_Lw__=2XXW0J4N117Q;+.ASPXAUTH=X6DUSPR2R53VZ53G  /product/FPR477BM http://www.google.com
   * 2012-04-27  00:08:24  __RequestVerificationToken_Lw__=3GQ426510U4H;+.ASPXAUTH=DJ4XNH6EMMW5CCC5  /product/NL0ZJO2L http://www.healthyshopping.com/product/N19C4MX1
   * 2012-04-27  00:09:31  __RequestVerificationToken_Lw__=3GQ426510U4H;+.ASPXAUTH=DJ4XNH6EMMW5CCC5  /addToCart/NL0ZJO2L /product/NL0ZJO2L
   * 2012-04-27  00:09:35  __RequestVerificationToken_Lw__=2XXW0J4N117Q;+.ASPXAUTH=X6DUSPR2R53VZ53G  /addToCart/FPR477BM /product/FPR477BM
   * 2012-04-27  00:09:45  __RequestVerificationToken_Lw__=UJAQ1TQWAGVL;+.ASPXAUTH=C142FL33KKCV603E  /product/7Y4FP655 http://www.twitter.com
   * 
   */
  def extract(sc:SparkContext,log:String):RDD[(String,Long,String,String,String,String)] = {
    
    sc.textFile(log).map(line => {
      
      val items = line.split(FIELD_DELIM)
      /*
       * Date & time are different fields in a log file entry and
       * are aggregated here
       */
      val date = dateFormat.parse(items(fieldspec(FIELD_DATE)) + " " + items(fieldspec(FIELD_TIME)))
      val timestamp = date.getTime()

      /*
       * Cookie evaluation
       */
      val citems = items(fieldspec(FIELD_COOKIE)).split(COOKIE_DELIM)
   
      val sessionid = citems.filter(cookieItem => cookieItem.startsWith(SESSION_ID_NAME))
          .map(cookieItem => cookieItem.split("=")(1)).head

      val userid = citems.filter(cookieItem => cookieItem.startsWith(USER_ID_NAME))
          .map(cookieItem => cookieItem.split("=")(1)).head
          
      (sessionid,timestamp,userid,items(fieldspec(FIELD_URL)),items(fieldspec(FIELD_TIME)),items(fieldspec(FIELD_REFERRER)))
      
    })
  
  }
 
}
