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

import java.util.regex.{Matcher,Pattern}

import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap

object Configuration {

  private val path = "application.conf"
  private val conf = ConfigFactory.load(path).getConfig("log")

  private val settings = fromConf()

  private def fromConf(): Map[String,String] = {
  
    Map(
      /*
       * Main settings to split a log line into a set of fields
       */
      "field.delim" -> conf.getString("field.delim"),
      "field.meta"   -> conf.getString("field.meta"),
      /*
       * Specific field evaluation
       */
      "session.id.name" -> conf.getString("session.id.name"),
      
      "user.id.name" -> conf.getString("user.id.name"),
      "cookie.separator" -> conf.getString("cookie.separator"),
      /*
       * Conversion indicator
       */
      "flow.sequence" -> conf.getString("flow.sequence")
    )
    
  }
  
  def get:Map[String,String] = settings

}
