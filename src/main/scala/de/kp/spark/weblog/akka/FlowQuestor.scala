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

import akka.actor.Actor

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.conf.{Configuration => HadoopConf}

import de.kp.spark.weblog.{Configuration,LogInsight,LogStatus}
import de.kp.spark.weblog.{InsightResponse,FlowInsightRequest}

import java.util.{Date,UUID}

class FlowQuestor extends Actor with SparkActor {
  /*
   * Directory on the file system to read
   * and write from during the mining tasks
   */
  private val MDIR = Configuration.MINING_DIR
  
  /*
   * Specification of Spark specific system properties
   */
  private val props = Map(
    "spark.executor.memory"          -> "1g",
	"spark.kryoserializer.buffer.mb" -> "256"
  )
  /*
   * Create Spark context
   */
  private val sc = createLocalCtx("FlowQuestor",props)
  
  def receive = {

    case req:FlowInsightRequest => {
      
      val origin = sender      
      
      val path  = req.filename
      val query = req.statement

      val rows = LogInsight.fromFlows(sc, path, query)
      origin ! new InsightResponse(Some(rows),LogStatus.SUCCESS)
      
      sc.stop
      context.stop(self)  
      
    }
    
    case _ => {}
  
  }
}