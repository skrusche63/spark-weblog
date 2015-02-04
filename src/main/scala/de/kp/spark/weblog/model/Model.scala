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
 * LogFlow specifies a summary of a certain session; as an added value, the
 * total number of page visits (total), the session duration (timespent), and
 * the flow status with respect to a specific conversion flow is computed.
 * 
 * In this project, the LogFlow is used to correlate the number of page visits
 * with the flow status of the session and train a Bayesian model to predict the
 * conversion probability from the number of clicks.
 * 
 * Beyond Web Analytics Example:
 * 
 * In addition to these first analytics results, a session can be converted
 * into a Markovian state, and the probability for following states can be
 * derived. Starting from these states, Intent Recognition and also Outlier
 * Detection can be used for deeper insight into a web session.
 *  
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

/**
 * LogPage specifies a certain page visit within web session; as an 
 * added value, the visit duration (timespent) and a rating derived 
 * from the time spent is computed.
 */
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

case class ClickPrediction(clicks:Int,probability:Double)

case class ClickPredictions(items:List[ClickPrediction])

object FlowStatus {

  /*
   * A set of indicators to specified whether 
   * a certain conversion has been achieved
   */
  val FLOW_NOT_ENTERED:Int = 0  
  val FLOW_ENTERED:Int     = 1
  val FLOW_COMPLETED:Int   = 2
  
}

object Messages extends BaseMessages {
 
  def MODEL_TRAINING_STARTED(uid:String):String = 
    String.format("""[UID: %s] Model training started.""", uid)

}

object ResponseStatus extends BaseStatus

object Serializer extends BaseSerializer {

  def serializeClickPredictions(predictions:ClickPredictions):String = write(predictions)
  def deserializeClickPredictions(predictions:String):ClickPredictions = read[ClickPredictions](predictions)

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
