package de.kp.spark.weblog.goal
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

import de.kp.spark.weblog.Configuration
import de.kp.spark.weblog.Configuration._

import de.kp.spark.weblog.model._

import scala.xml._

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.util.control.Breaks._

/**
 * Conversion goals are specified as a sequence (flow) of 
 * page views; the Goals object loads these pred-defined
 * conversion goals from the local file system and determines,
 * whether a certain sequence of page clicks matches a goal
 * or even not.
 */
object Goals {

  private val path = "goals.xml"
  private val flows = build
  
  private def build:HashMap[String,Array[String]] = {

    val data = HashMap.empty[String,Array[String]]
    val root = XML.load(getClass.getClassLoader.getResource(path))     

    for (goal <- root \ "goal") {
      
      val fid  = (goal \ "@id").toString
      val flow = goal.text.split(",")
      
      data += fid -> flow
      
    }

    data
    
  }

  def getFlow(fid:String):Option[Array[String]] = {
    flows.get(fid)
  }
  
  def getFlows():Array[(String,Array[String])] = {
    flows.toArray
  }
  
  /**
   * A helper method to evaluate whether the pages clicked in a certain 
   * session match, partially match or do not match a predefined sequence
   * of pages (flow)
   */
  def checkFlow(pages:List[String],flow:Array[String]):Int = { 			
    		
    var j = 0
    var	flowStat = FlowStatus.FLOW_NOT_ENTERED
    		
    var matched = false;
    		
    for (i <- 0 until flow.length) {
    			
      breakable {while (j < pages.size) {
    				
        matched = false
        /*
         * We expect that a certain page url has to start with the 
         * configured url part of the flow
         */
    	if (pages(j).startsWith(flow(i))) {
    	  flowStat = (if (i == flow.length - 1) FlowStatus.FLOW_COMPLETED else FlowStatus.FLOW_ENTERED)
    	  matched = true
    				
    	}
    	j += 1
    	if (matched) break
    			
      }}
    
    }

    flowStat
    
  }
  
  /**
   * A helper method to evaluate whether the pages clicked in a certain 
   * session match, partially match or do not match a predefined sequences
   * of page flows
   */
  def checkFlows(pages:List[String]):Array[(String,Int)] = { 			
    
    val flows = Goals.getFlows
    flows.map(v => (v._1, checkFlow(pages,v._2)))
    
  }
 
}