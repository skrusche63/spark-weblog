package de.kp.spark.weblog.api
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

import java.util.Date

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.spark.weblog.actor.WeblogMaster
import de.kp.spark.weblog.Configuration

class RestApi(host:String,port:Int,system:ActorSystem,@transient sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new WeblogMaster(sc)), name="weblog-master")
 
  private val service = "weblog"
    
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {

    path("get" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("train" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,subject)
	    }
	  }
    }
    
  }

  private def doGet[T](ctx:RequestContext,subject:String) = {
    
    val task = "get:" + subject
    
    val topics = List("click","flow","page")
    if (topics.contains(subject)) doRequest(ctx,"",task)
    
    
  }
  /**
   * The weblog REST service actually supports three different models, which are
   * all derived from a W3 compliant web log file:
   * 
   * 'click': this model computes the correlation of page clicks in a session
   * and conversions with respect to a certain pre-defined conversion goal.
   * 
   * 'flow': this 'model' derives characteristic attributes for web sessions
   * and stores this aggregated information as a parquet file on HDFS. The data
   * are used to apply SQL-like queries, and can also be used with other analysis
   * tasks from Predictiveworks.
   * 
   * 'page': this 'model' derives characteristic attributes for web pages within
   * a certain session and and stores this aggregated information as a parquet file 
   * on HDFS. The data are used to apply SQL-like queries, and can also be used with 
   * other analysis tasks from Predictiveworks.
   */
  private def doTrain[T](ctx:RequestContext,subject:String) = {
    
    val task = "train:" + subject
    
    val topics = List("click","flow","page")
    if (topics.contains(subject)) doRequest(ctx,"",task)
    
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}