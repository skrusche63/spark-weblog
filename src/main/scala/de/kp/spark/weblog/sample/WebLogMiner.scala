package de.kp.spark.weblog.sample
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

import de.kp.spark.weblog.{LogExtractor,LogEvaluator,LogModel}

/**
 * The WebLogMiner performs a sequence of data mining task
 * on a W3C complaint web server log file
 */
object WebLogMiner extends SparkApp {

  def main(args:Array[String]) {
    
    val sc = createLocalCtx("WebLogMiner")
    /*
     * Directory on the file system to read
     * and write from during the mining tasks
     */
    val path = "/Work/tmp/web-log/"
    /*
     * Preparation step to extract configured data fields
     * from a web log file and return in a structured way
     */
    val sessions = LogExtractor.extract(sc, path + "w3logfile.txt")
    /*
     * Mining #1: Compute time spent on each page within a session
     * and rate these pages with respect to the time spent.
     * 
     * This is an individual mining tasks that has no successor;
     * therefore all the data stored in the file system
     */
    val pages = LogEvaluator.eval1(sessions)
    pages.map(p => LogModel.serializePage(p)).saveAsTextFile(path + "pages")
    
  }
}