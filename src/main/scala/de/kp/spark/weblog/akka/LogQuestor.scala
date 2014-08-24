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

import com.typesafe.config.ConfigFactory

import de.kp.spark.weblog.{FlowInsightRequest,PageInsightRequest,InsightResponse,LogStatus}

import scala.concurrent.duration.DurationInt

class LogQuestor extends Actor with ActorLogging {

  val path = "application.conf"
  val config = ConfigFactory.load(path)
  
  val cfg = config.getConfig("insight")  
  val time = cfg.getInt("time")

  def receive = {
    
    case req:FlowInsightRequest => {
      
      implicit val ec = context.dispatcher
      implicit val timeout:Timeout = DurationInt(time).second

      val origin = sender
      val questor = context.actorOf(Props[FlowQuestor])
	  
      val response = ask(questor, req).mapTo[InsightResponse]
      response.onSuccess {
        case result => origin ! result       
      }
      response.onFailure {
        case result => origin ! new InsightResponse(None,LogStatus.FAILURE)	      
	  }
      
    }
    
    case req:PageInsightRequest => {

      implicit val ec = context.dispatcher
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
      val origin = sender
      val questor = context.actorOf(Props[PageQuestor])
	  
      val response = ask(questor, req).mapTo[InsightResponse]
      response.onSuccess {
        case result => {
          println("PageRequestor: " + result)
          origin ! result
        }     
      }
      response.onFailure {
        case result => origin ! new InsightResponse(None,"failure")	     
	  }
      
    }
    
    
  }
 
}
