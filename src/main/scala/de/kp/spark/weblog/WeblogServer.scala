package de.kp.spark.weblog
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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.weblog.api.AkkaApi

object WeblogService extends SparkService {
  
  private val sc = createCtxLocal("WeblogContext",Configuration.spark)      

  def main(args: Array[String]) {
    
    /**
     * AKKA API 
     */
    val conf:String = "server.conf"

    val akkaSystem = ActorSystem("akka-server",ConfigFactory.load(conf))
    sys.addShutdownHook(akkaSystem.shutdown)
    
    new AkkaApi(akkaSystem,sc).start()
 
    println("AKKA API activated.")
      
  }

}