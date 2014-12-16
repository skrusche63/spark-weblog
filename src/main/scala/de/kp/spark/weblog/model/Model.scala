package de.kp.spark.weblog.model
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

import de.kp.spark.core.model._

/**
 * DATA MODEL
 */
case class LogFlow(
  sessid:String,
  userid:String,
  total:Int,
  starttime:Long,
  timespent:Long,
  referrer:String,
  exitpage:String,
  flowstatus:Int
)

case class LogFlows(items:List[LogFlow])

case class LogPage (
  sessid:String,
  userid:String,
  starttime:Long,
  pageurl:String,
  visittime:String,
  referrer:String,
  timespent:Long,
  rating:Int
)

case class LogPages(items:List[LogPage])

object Messages extends BaseMessages {
 
  def MODEL_TRAINING_STARTED(uid:String):String = 
    String.format("""[UID: %s] Model training started.""", uid)

}

object ResponseStatus extends BaseStatus

object Serializer extends BaseSerializer {

  def serializeLogFlows(flows:LogFlows):String = write(flows)
  def deserializeLogFlows(flows:String):LogFlows = read[LogFlows](flows)

  def serializeLogPages(pages:LogPages):String = write(pages)
  def deserializeLogPages(pages:String):LogPages = read[LogPages](pages)
  
}

object Sinks {

  val FILE:String    = "FILE"
  val PARQUET:String = "PARQUET"    
    
  private val sinks = List(FILE,PARQUET)  
  
  def isSink(sink:String):Boolean = sinks.contains(sink)
  
}

object Sources {

  val FILE:String    = "FILE"
  val PARQUET:String = "PARQUET"    
    
  private val sources = List(FILE,PARQUET)  
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}
