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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

import de.kp.spark.weblog.model._
import scala.collection.mutable.HashMap

class W3LogQueries(@transient sc:SparkContext) extends Serializable {
  
  /**
   * Load session flow description from file system and apply query
   */
  def fromFlows(path:String,query:String):List[LogFlow] = {
    
    val (rows,metadata) = fromFile(path,query,"flows")
    rows.map(row => rowToFlow(row,metadata)).toList
  
  }
  
  private def rowToFlow(row:Row,metadata:Seq[(StructField,Int)]):LogFlow = {

    val data = rowToMap(row,metadata)
    
    val sessid = if (data.contains("sessid")) data("sessid").asInstanceOf[String] else ""
    val userid = if (data.contains("userid")) data("userid").asInstanceOf[String] else ""

    val total = if (data.contains("total")) data("total").asInstanceOf[Int] else 0

    val starttime = if (data.contains("starttime")) data("starttime").asInstanceOf[Long] else 0
    val timespent = if (data.contains("timespent")) data("timespent").asInstanceOf[Long] else 0

    val referrer = if (data.contains("referrer")) data("referrer").asInstanceOf[String] else ""
    val exitpage = if (data.contains("exitpage")) data("exitpage").asInstanceOf[String] else ""

    val flowstatus = if (data.contains("flowstatus")) data("flowstatus").asInstanceOf[Int] else 0
    
    LogFlow(sessid,userid,total,starttime,timespent,referrer,exitpage,flowstatus)

  }
  
  /**
   * Load web page description from file system and apply query
   */
  def fromPages(path:String,query:String):List[LogPage] = {
  
    val (rows,metadata) = fromFile(path,query,"pages")
    rows.map(row => rowToPage(row,metadata)).toList
  
  }
  
  private def rowToPage(row:Row,metadata:Seq[(StructField,Int)]):LogPage = {

    val data = rowToMap(row,metadata)
    
    val sessid = if (data.contains("sessid")) data("sessid").asInstanceOf[String] else ""
    val userid = if (data.contains("userid")) data("userid").asInstanceOf[String] else ""

    val starttime = if (data.contains("starttime")) data("starttime").asInstanceOf[Long] else 0
    val pageurl = if (data.contains("pageurl")) data("pageurl").asInstanceOf[String] else ""

    val visittime = if (data.contains("visittime")) data("visittime").asInstanceOf[String] else ""
    val referrer = if (data.contains("referrer")) data("referrer").asInstanceOf[String] else ""
      
    val timespent = if (data.contains("timespent")) data("timespent").asInstanceOf[Long] else 0
    val rating = if (data.contains("rating")) data("rating").asInstanceOf[Int] else 0

    LogPage(sessid,userid,starttime,pageurl,visittime,referrer,timespent,rating)
    
  }
  
  private def rowToMap(row:Row,metadata:Seq[(StructField,Int)]):Map[String,Any] = {

    val data = HashMap.empty[String,Any]
    val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
    
    metadata.foreach(entry => {
      
      val field = entry._1
      val col   = entry._2
      
      val colname = field.name
      val colvalu = values(col)
      
      data += colname -> colvalu
    
    })
    
    data.toMap
    
  }

  /**
   * Apply query to in-memory session flow descriptions
   */
  def fromFlows(source:RDD[LogFlow],query:String):Array[Row] = {
    
    val sqlc = new SQLContext(sc)
    val schema = sqlc.createSchemaRDD(source)
    
    val pages = sqlc.registerRDDAsTable(schema, "flows")

    sqlc.sql(query).collect()    

  }

  /**
   * Apply query to in-memory web page descriptions
   */
  def fromPages(source:RDD[LogPage],query:String):Array[Row] = {
    
    val sqlc = new SQLContext(sc)
    val schema = sqlc.createSchemaRDD(source)
    
    val pages = sqlc.registerRDDAsTable(schema, "pages")

    sqlc.sql(query).collect()    
    
  }

  private def fromFile(path:String,query:String,table:String):(Array[Row],Seq[(StructField,Int)]) = {
    
    val sqlc = new SQLContext(sc)
    import sqlc.createSchemaRDD
    
    val parquetFile = sqlc.parquetFile(path)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.registerTempTable(table)
    val rows = sqlc.sql(query).collect()    

    (rows,metadata)
    
  }

}