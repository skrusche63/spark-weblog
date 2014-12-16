package de.kp.spark.weblog.actor
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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.weblog.Configuration
import de.kp.spark.weblog.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class W3LogMiner(@transient sc:SparkContext) extends BaseActor {

  implicit val ec = context.dispatcher
 
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender          
      val response = try {
        
        train(req).mapTo[ServiceResponse]
      
      } catch {
        case e:Exception => {
          Future {failure(req,e.getMessage)}          
        }
      }
      
      response.onSuccess {
        
        case result => {
          origin ! result
          context.stop(self)
        }
      
      }

      response.onFailure {
        
        case throwable => {           
          origin ! failure(req,throwable.toString)	                  
          context.stop(self)
        }	  
      
      }
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }

  private def train(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }

  private def actor(req:ServiceRequest):ActorRef = {
    
    val Array(task,topic) = req.task.split(":")
    topic match {

      case "click" => context.actorOf(Props(new W3ClickMiner(sc)))
      
      case "flow" => context.actorOf(Props(new W3FlowMiner(sc)))
      case "page" => context.actorOf(Props(new W3PageMiner(sc)))
      
      case _ => null
      
    }
    
  }
  
}
