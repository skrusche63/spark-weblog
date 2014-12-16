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

import de.kp.spark.weblog.{BayesianPredictor,W3LogEvaluator}
import de.kp.spark.weblog.model._

import de.kp.spark.weblog.source.W3LogSource
import de.kp.spark.weblog.sink.W3LogSink

class W3ClickMiner(@transient sc:SparkContext) extends BaseActor {
 
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)
          
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MODEL_TRAINING_STARTED(uid))
      val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.MODEL_TRAINING_STARTED)	
      
      origin ! response
      
      try {
        train(req)
      
      } catch {
        case e:Exception => cache.addStatus(req,ResponseStatus.FAILURE)          
      }
          
      context.stop(self)
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }

  private def train(req:ServiceRequest) {

    /* Register status */
    cache.addStatus(req,ResponseStatus.MODEL_TRAINING_STARTED)
    /*
     * Preparation step to extract configured data fields
     * from a web log file and return in a structured way
     */
    val sessions = new W3LogSource(sc).get(req)
    if (sessions == null) throw new Exception("Data source is empty.")
    /*
     * Compute aggregated and checkout specific data for each web session 
     * or visit; the result is stored on the file system
     */
    val dataset = new W3LogEvaluator().logFlows(sessions)    
    val model = BayesianPredictor.train(dataset)
    
    new W3LogSink(sc).saveClickModel(req, model)
    
  }
  
}