package de.kp.spark.weblog.source
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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.weblog.Configuration._

class W3LogModel(@transient sc:SparkContext) extends Serializable {

  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[(String,Long,String,String,String,String)] = {
    
    rawset.map(line => {
      
      val items = line.split(FIELD_DELIM)
      /*
       * Date & time fields in a log file entry are aggregated to a timestamp: 2012-04-27  00:07:40
       */
      val date = dateFormat.parse(items(fieldspec(FIELD_DATE)) + " " + items(fieldspec(FIELD_TIME)))
      val timestamp = date.getTime()

      /*
       * Extract session and user identifier from cookie: e.g. from IIS web server
       * __RequestVerificationToken_Lw__=3GQ426510U4H;+.ASPXAUTH=DJ4XNH6EMMW5CCC5
       */
      val citems = items(fieldspec(FIELD_COOKIE)).split(COOKIE_DELIM)
      val sessionid = citems.filter(cookieItem => cookieItem.startsWith(SESSION_ID_NAME))
          .map(cookieItem => cookieItem.split("=")(1)).head

      val userid = citems.filter(cookieItem => cookieItem.startsWith(USER_ID_NAME))
          .map(cookieItem => cookieItem.split("=")(1)).head
      
      /* Format: (sessionid,timestamp,userid,pageurl,visittime,referrer) */
      (sessionid,timestamp,userid,items(fieldspec(FIELD_URL)),items(fieldspec(FIELD_TIME)),items(fieldspec(FIELD_REFERRER)))
      
    })

  }

  def buildParquet(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,Long,String,String,String,String)] = {
    null
  }
  
}