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

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.weblog.Configuration

class WeblogMaster(@transient sc:SparkContext) extends BaseMaster(Configuration) {
  
  protected def actor(worker:String):ActorRef = {
    
    worker match {
      
      case "get"   => context.actorOf(Props(new W3LogQuestor(sc)))
      case "train" => context.actorOf(Props(new W3LogMiner(sc)))
       
      case _ => throw new Exception("Task is unknown.")
       
    }
  
  }

}