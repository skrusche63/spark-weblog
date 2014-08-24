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

import scala.concurrent.ExecutionContext.Implicits.global

import de.kp.spark.weblog.{PageInsightRequest,InsightResponse}
import de.kp.spark.weblog.akka.LogClient

object SimpleInsightApp {

  def main(args:Array[String]) {
    
    val path  = "/Work/tmp/web-log/pages"
    val query = "select * from pages"
      
    val req = new PageInsightRequest(path,query)
    val client = new LogClient()
    
    client.send(req)
     
    val response = client.send(req).mapTo[InsightResponse]
    response.onSuccess {
        case result => {
          
          result.rows match {
            
            case None => println("No data retrieved")
            case Some(rows) => rows.foreach(r => println(r))
            
          }
          
          
        }
      }
      response.onFailure {
        case result => println(result)      
	  }
    
  }
}