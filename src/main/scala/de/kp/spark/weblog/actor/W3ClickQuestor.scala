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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.weblog.{Configuration}
import de.kp.spark.weblog.model._

import de.kp.spark.weblog.sink.W3LogSink

class W3ClickQuestor(@transient sc:SparkContext) extends BaseActor {
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender      

      val response = try {
      
        val clicks = req.data("clicks").toInt
        val model = new W3LogSink(sc).getClickModel(req)
        
        val prediction = model.predict(clicks)
        
        val data = Map(Names.REQ_UID -> req.data(Names.REQ_UID), Names.REQ_RESPONSE -> prediction.toString)
        new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
      
      } catch {
        case e:Exception => failure(req,e.getMessage)
      }
      
      origin ! response
      context.stop(self)  
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
}