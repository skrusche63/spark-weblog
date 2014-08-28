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

  /*
   * LOG FILE CONFIGURATION
   */
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
  
  val FLOW_SEQUENCE = "flow.sequence"
    
  val PAGE_RATING = "pagetime.rating"

  /*
   * A set of indicators to specified whether 
   * a certain conversion has been achieved
   */
  val FLOW_NOT_ENTERED = 0  
  val FLOW_ENTERED     = 1
  val FLOW_COMPLETED   = 2
  
  /* Conversion file */
  private val GOAL_PATH = "path"    
  /* Mining directory */
  private val MINING_PATH = "path"
  
  
  private val path = "application.conf"
  private val conf = ConfigFactory.load(path)

  private val conversionProps = fromConversionCfg()

  private val logfileProps = fromLogfileCfg()
  private val miningProps  = fromMiningCfg()

  private def fromConversionCfg(): Map[String,String] = {

    val cfg = conf.getConfig("conversion")
    Map(GOAL_PATH -> cfg.getString(GOAL_PATH))
    
  }

  private def fromLogfileCfg(): Map[String,String] = {
  
    val cfg = conf.getConfig("logfile")
    
    Map(
      /*
       * Main settings to split a log line into a set of fields
       */
      FIELD_DELIMITER -> cfg.getString(FIELD_DELIMITER),
      FIELD_METADATA  -> cfg.getString(FIELD_METADATA),
      /*
       * Specific field evaluation
       */
      FIELD_SESSIONID -> cfg.getString(FIELD_SESSIONID),
      
      FIELD_USERID -> cfg.getString(FIELD_USERID),
      COOKIE_DELIMITER -> cfg.getString(COOKIE_DELIMITER),
      
      PAGE_RATING -> cfg.getString(PAGE_RATING),
      
      /*
       * Conversion indicator
       */
      FLOW_SEQUENCE -> cfg.getString(FLOW_SEQUENCE)
    )
    
  }

  private def fromMiningCfg(): Map[String,String] = {

    val cfg = conf.getConfig("mining")
    Map(MINING_PATH -> cfg.getString(MINING_PATH))
    
  }
  /*
   * Data format
   */
  def dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def fieldspec:Map[String,Int] = {
    
    logfileProps(FIELD_METADATA).split(",").map(valu => {
      
      val Array(name,pos) = valu.split(":")
      (name,pos.toInt)
    
    }).toMap
    
  }

  def flow = logfileProps(FLOW_SEQUENCE).split(",")
  
  def ratings:Map[Int,Int] = {
    
    logfileProps(PAGE_RATING).split(",").map(valu => {
      
      val Array(timespent,rating) = valu.split(":")
      (timespent.toInt,rating.toInt)
    
    }).toMap

  }
  
  def config:Map[String,String] = logfileProps

  def COOKIE_DELIM = logfileProps(COOKIE_DELIMITER)
  
  def FIELD_DELIM  = logfileProps(FIELD_DELIMITER)

  def SESSION_ID_NAME = logfileProps(FIELD_SESSIONID)
  
  def USER_ID_NAME = logfileProps(FIELD_USERID)

  /*
   * Returns a specific XML file on the file system
   * that describes all conversion goals
   */
  def GOAL_FILE = conversionProps(GOAL_PATH)

  /*
   * Returns the specified directory on the file system
   * where to read and write mining results
   */
  def MINING_DIR = miningProps(MINING_PATH)
  
}
