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

import de.kp.spark.weblog.{Configuration,LogEvaluator,LogExtractor,LogModel,PageMiningTask}

class MinerWorker extends Actor with SparkActor {
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
  private val sc = createLocalCtx("MinerWorker",props)
  
  def receive = {
    /*
     * The PageMiningTask is responsible for extracting relevant
     * information from a W3C compliant web log and computing
     * the time spent on each page assigned with a page rating
     * 
     * The mining result is written to the file system
     */
    case req:PageMiningTask => {
      /*
       * Preparation step to extract configured data fields
       * from a web log file and return in a structured way
       */
      val logfile = MDIR + req.filename
      val sessions = LogExtractor.extract(sc, logfile)
      /*
       * Mining #1: Compute time spent on each page within a session
       * and rate these pages with respect to the time spent.
       * 
       * This is an individual mining tasks that has no successor;
       * therefore all the data stored in the file system
       */
      val pagefile = (MDIR + "pages")
      
      val fs = FileSystem.get(new HadoopConf())      
      fs.delete(new Path(pagefile), true)     
      
      val pages = LogEvaluator.eval1(sessions)
      pages.map(p => LogModel.serializePage(p)).saveAsTextFile(pagefile)
      
    }
    
    case _ => {}
  
  }
  
}