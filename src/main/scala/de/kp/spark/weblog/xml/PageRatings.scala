package de.kp.spark.weblog.xml
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
import scala.xml._
import scala.collection.mutable.HashMap

object PageRatings {
  
  private val path = "page_ratings.xml"
  val values = build
  
  private def build:HashMap[Int,Int] = {
    
    val data = HashMap.empty[Int,Int]
    val root = XML.load(getClass.getClassLoader.getResource(path))     
    
    for (field <- root \ "rating") {
      
      val timespent  = (field \ "timespent").text.toInt
      val score      = (field \ "score").text.toInt

      data += timespent -> score
      
    }
  
    data
    
  }
  
  def get(timespent:Int):Int = values(timespent)

}