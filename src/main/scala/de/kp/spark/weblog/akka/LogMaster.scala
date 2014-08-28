package de.kp.spark.weblog.akka
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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import com.typesafe.config.ConfigFactory

import de.kp.spark.weblog.{FlowInsightRequest,FlowMiningRequest,PageInsightRequest,PageMiningRequest,InsightResponse,MiningStarted,MiningCompleted}

import scala.concurrent.duration.DurationInt

class LogMaster extends Actor with ActorLogging {
  /*
   * Load configuration for router
   */
  val path = "application.conf"
  val config = ConfigFactory.load(path)
  
  val routercfg = config.getConfig("router")
  
  val time    = routercfg.getInt("time")
  val retries = routercfg.getInt("retries")  
  val workers = routercfg.getInt("workers")
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val logminer   = context.actorOf(Props[LogMiner])
  val logquestor = context.actorOf(Props[LogQuestor].withRouter(RoundRobinRouter(workers)))
    
  def receive = {
    
    /*
     * Insight support
     */
    case req:FlowInsightRequest => {

      implicit val ec = context.dispatcher

      val time = config.getConfig("insight").getInt("time")      
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
      val origin = sender
      val response = ask(logquestor, req).mapTo[InsightResponse]
      
      response.onSuccess {
        case result => {
          println("FlowInsightRequestor: " + result)
          origin ! result}       
      }
      response.onFailure {
        case result => origin ! new InsightResponse(None,"failed")		      
	  }
      
    }
    case req:PageInsightRequest => {

      implicit val ec = context.dispatcher

      val time = config.getConfig("insight").getInt("time")      
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender
      val response = ask(logquestor, req).mapTo[InsightResponse]
      
      response.onSuccess {
        case result => {
          println("PageInsightRequestor: " + result)
          origin ! result
        }       
      }
      response.onFailure {
        case result => origin ! new InsightResponse(None,"failed")	      
	  }

	}
   
    /*
     * Mining support
     */
    case req:FlowMiningRequest => {}
    case req:PageMiningRequest => {}
    
    
  }

}