package de.kp.spark.weblog.source
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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.source._

import de.kp.spark.weblog.Configuration
import de.kp.spark.weblog.model._

class W3LogSource(@transient sc:SparkContext) {

  private val config = Configuration
  private val model = new W3LogModel(sc)
  
  def get(req:ServiceRequest):RDD[(String,Long,String,String,String,String)] = {
 
    val source = req.data(Names.REQ_SOURCE)
    source match {
 
      case Sources.FILE => {
       
        val rawset = new FileSource(sc).connect(config.input(0),req)
        model.buildFile(req,rawset)
        
      }
 
      case Sources.PARQUET => {
       
        val rawset = new ParquetSource(sc).connect(config.input(0),req,List.empty[String])
        model.buildParquet(req,rawset)
        
      }
    
      case _ => throw new Exception("The input data source is not supported.")
    }
  
  }
  
}