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

import org.apache.spark.sql.Row

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class MiningStarted (
  uid:String,
  timestamp:Long,
  status:String
)

case class MiningCompleted (
  uid:String,
  timestamp:Long,
  status:String
)

case class FlowInsightRequest(
  filename:String,
  statement:String
)

/**
 * A task to extract checkout & conversion based information
 * from the web server log file
 */
case class FlowMiningRequest (
  /*
   * Name of web server log file
   */
  filename:String
)

case class PageInsightRequest(
  filename:String,
  statement:String
)

/**
 * A task to extract relevant web page based information
 * from the web server log file
 */
case class PageMiningRequest (
  /*
   * Name of web server log file
   */
  filename:String
)

case class InsightResponse(
  rows:Option[Array[Row]],
  status:String
)

object LogStatus {
  
  val SUCCESS = "success"
  val FAILURE = "failure"
    
}