package de.kp.spark.weblog.akka
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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import akka.contrib.pattern.Aggregator

import de.kp.spark.weblog.{FlowMiningRequest,PageMiningRequest,MiningStarted,MiningCompleted}

class LogMiner extends Actor with ActorLogging with Aggregator {

  expectOnce {
    
    case req:FlowMiningRequest => new FlowRequestor(sender, req)
    case req:PageMiningRequest => new PageRequestor(sender, req)
  
  }
  
  class FlowRequestor(origin:ActorRef,req:FlowMiningRequest) {
    
    process(req)
    
	def process(req:FlowMiningRequest) {
	  	    
	  context.actorOf(Props[FlowMiner]) ! req
      expectOnce {
	    
	    case res:MiningStarted   => origin ! res
	    case res:MiningCompleted => origin ! res
	    
	  }

	}
  
  }
  
  class PageRequestor(origin:ActorRef,req:PageMiningRequest) {
    
    process(req)
    
	def process(req:PageMiningRequest) {
	  	    
	  context.actorOf(Props[PageMiner]) ! req
      expectOnce {
	    
	    case res:MiningStarted   => origin ! res
	    case res:MiningCompleted => origin ! res
	    
	  }

	}
    
  }

}
