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

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.{Configuration => HConf}

import de.kp.spark.core.{Configuration => CoreConf}

import java.text.SimpleDateFormat

import scala.collection.mutable.HashMap

object Configuration extends CoreConf {
  
  private val path = "application.conf"
  private val config = ConfigFactory.load(path)

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

  private val logfileProps = fromLogfileCfg()

  private def fromLogfileCfg(): Map[String,String] = {
  
    val cfg = config.getConfig("logfile")
    
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
      
      /*
       * Conversion indicator
       */
      FLOW_SEQUENCE -> cfg.getString(FLOW_SEQUENCE)
    )
    
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
  
  def getLogFileProps:Map[String,String] = logfileProps

  def COOKIE_DELIM = logfileProps(COOKIE_DELIMITER)
  
  def FIELD_DELIM  = logfileProps(FIELD_DELIMITER)

  def SESSION_ID_NAME = logfileProps(FIELD_SESSIONID)
  
  def USER_ID_NAME = logfileProps(FIELD_USERID)

  override def actor:(Int,Int,Int) = {
  
    val cfg = config.getConfig("actor")

    val duration = cfg.getInt("duration")
    val retries = cfg.getInt("retries")  
    val timeout = cfg.getInt("timeout")
    
    (duration,retries,timeout)
    
  }
 
  override def cassandra:Map[String,String] = {
   
    val cfg = config.getConfig("cassandra")
    val conf = Map(
      "spark.cassandra.connection.host" -> cfg.getString("spark.cassandra.connection.host")
    )                          

    conf
     
  }

  override def elastic:HConf = null
 
  override def hbase:Map[String,String] = null
  
  override def input:List[String] = {
  
    val cfg = config.getConfig("input")
    List(cfg.getString("path"))   
    
  }
 
  override def mongo:HConf = {
   
    val cfg = config.getConfig("mongo")
    val conf = new HConf()                          

    conf.set("mongo.input.uri",cfg.getString("mongo.input.uri"))
    conf
     
  }
 
  override def mysql:(String,String,String,String) = null
  
  override def output:List[String] = {
  
    val cfg = config.getConfig("output")
    List(cfg.getString("path"))   
    
    
  }
  
  override def redis:(String,String) = {
  
    val cfg = config.getConfig("redis")
    
    val host = cfg.getString("host")
    val port = cfg.getString("port")
    
    (host,port)
    
  }

  override def rest:(String,Int) = {
      
    val cfg = config.getConfig("rest")
      
    val host = cfg.getString("host")
    val port = cfg.getInt("port")

    (host,port)
    
  }
  
  override def spark:Map[String,String] = {
  
    val cfg = config.getConfig("spark")
    
    Map(
      "spark.executor.memory"          -> cfg.getString("spark.executor.memory"),
	  "spark.kryoserializer.buffer.mb" -> cfg.getString("spark.kryoserializer.buffer.mb")
    )

  }
  
}
