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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

/**
 * A task to extract relevant web page based information
 * from the web server log file
 */
case class PageMiningTask(
  /*
   * Name of web server log file
   */
  filename:String
)

/**
 * LogPage specifies a certain page url within a user session
 */
case class LogPage(
  sessid:String,
  userid:String,
  starttime:Long,
  pageurl:String,
  visittime:String,
  referrer:String,
  timespent:Long,
  rating:Int
)

/**
 * LogModel holds utility methods with respect
 * to extracted web log information
 */
object LogModel {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializePage(page:LogPage):String = write(page)
  
  def deserializePage(line:String):LogPage = read[LogPage](line)
  
}