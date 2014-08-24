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

import de.kp.spark.weblog.{Configuration,LogEvaluator,LogExtractor,LogModel,FlowMiningTask}

class FlowMiner extends Actor with SparkActor {
  /*
   * Directory on the file system to read
   * and write from during the mining tasks
   */
  private val MDIR = Configuration.MINING_DIR
  
  /*
   * Specification of Spark specific system properties
   */
  private val props = Map(
    "spark.executor.memory"          -> "4g",
	"spark.kryoserializer.buffer.mb" -> "256"
  )
  /*
   * Create Spark context
   */
  private val sc = createLocalCtx("FlowMiner",props)
  
  def receive = {
    /*
     * The FlowMiningTask is responsible for a) extracting relevant
     * information from a W3C compliant web log, and b) computing 
     * checkout specific data for each session
     * 
     * The mining result is written to the file system
     */
    case req:FlowMiningTask => {
      /*
       * Preparation step to extract configured data fields
       * from a web log file and return in a structured way
       */
      val logfile = MDIR + req.filename
      val sessions = LogExtractor.extract(sc, logfile)
      /*
       * Mining #2: Compute aggregated and checkout specific data
       * for each web session or visit
       * 
       * This is an individual mining tasks that has no successor;
       * therefore all the data stored in the file system
       */
      val flowfile = (MDIR + "flows")
      
      val fs = FileSystem.get(new HadoopConf())      
      fs.delete(new Path(flowfile), true)     
      
      val flows = LogEvaluator.eval2(sessions)
      flows.map(f => LogModel.serializeFlow(f)).saveAsTextFile(flowfile)
      
    }
    
    case _ => {}
  
  }
}