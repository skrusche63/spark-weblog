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

import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap

object Configuration {

  val FIELD_DELIMITER = "field.delim"
  val FIELD_METADATA  = "field.meta"
  
  /*
   * Field names 
   */
  val FIELD_COOKIE   = "cookie"  
  val FIELD_DATE     = "date"  
  val FIELD_REFERRER = "referrer"    
  val FIELD_TIME     = "time"  
  val FIELD_URL      = "url"  
  
  /*
   * Specific field names
   */  
  val FIELD_SESSIONID = "session.id.name"
  val FIELD_USERID    = "user.id.name"

  val COOKIE_DELIMITER = "cookie.delim"
    
  val PAGE_RATING = "pagetime.rating"
    
  private val path = "application.conf"
  private val conf = ConfigFactory.load(path).getConfig("log")

  private val settings = fromConf()

  private def fromConf(): Map[String,String] = {
  
    Map(
      /*
       * Main settings to split a log line into a set of fields
       */
      FIELD_DELIMITER -> conf.getString(FIELD_DELIMITER),
      FIELD_METADATA  -> conf.getString(FIELD_METADATA),
      /*
       * Specific field evaluation
       */
      FIELD_SESSIONID -> conf.getString(FIELD_SESSIONID),
      
      FIELD_USERID -> conf.getString(FIELD_USERID),
      COOKIE_DELIMITER -> conf.getString(COOKIE_DELIMITER),
      
      PAGE_RATING -> conf.getString(PAGE_RATING),
      
      /*
       * Conversion indicator
       */
      "flow.sequence" -> conf.getString("flow.sequence")
    )
    
  }

  /*
   * Data format
   */
  def dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def fieldspec:Map[String,Int] = {
    
    settings(FIELD_METADATA).split(";").map(valu => {
      
      val Array(name,pos) = valu.split(":")
      (name,pos.toInt)
    
    }).toMap
    
  }
  
  def ratings:Map[Int,Int] = {
    
    settings(PAGE_RATING).split(";").map(valu => {
      
      val Array(timespent,rating) = valu.split(":")
      (timespent.toInt,rating.toInt)
    
    }).toMap

  }
  
  def config:Map[String,String] = settings

  def COOKIE_DELIM = settings(COOKIE_DELIMITER)
  
  def FIELD_DELIM  = settings(FIELD_DELIMITER)

  def SESSION_ID_NAME = settings(FIELD_SESSIONID)
  
  def USER_ID_NAME    = settings(FIELD_USERID)

}
