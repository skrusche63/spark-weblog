![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

### Web Log Mining with Spark

Web Log Analysis is based on log files that are compliant to the W3C web server log format. The IIS is a prominant candidate that supports this format. The mining functionality itself is not restricted to this format and may be easily adapted to other log files by changing configuration parameters.

Web Log Mining is used here to extract common web analytic parameters.
![Spark-WebLog](https://raw.githubusercontent.com/skrusche63/spark-weblog/master/images/spark-weblog.png)

The [Spark-Piwik](https://github.com/skrusche63/spark-piwik) project integrates [Piwik Analytics](http://piwik.org) with Spark and e.g. predicts the purchase horizon from customer engagement events with Markov Models. These models may also be applied to data from W3C web server logs to predict valuable parameters.

---

### Insights from Web Logs

Spark and Spark SQL are big data (mining) tools, that make it very easy to retrieve fast insights from large-scale datasets. The following example describes how mining results, represented as Spark compliant data structures, are further evaluated by applying SQL queries. 

The dataset below is the result of a (first) mining task, applied to the web server log data:
```
sessionid | userid | timestamp | pageurl | visittime | referrer | timespent | rating
------------------------------------------------------------------------------------

DJ4XNH6EMMW5CCC5,3GQ426510U4H,1335478060000,/product/N19C4MX1,00:07:40,http://www.healthyshopping.com/product/T0YJZ1QH,44,6
DJ4XNH6EMMW5CCC5,3GQ426510U4H,1335478060000,/product/NL0ZJO2L,00:08:24,http://www.healthyshopping.com/product/T0YJZ1QH,67,6
DJ4XNH6EMMW5CCC5,3GQ426510U4H,1335478060000,/addToCart/NL0ZJO2L,00:09:31,http://www.healthyshopping.com/product/T0YJZ1QH,0,0
X6DUSPR2R53VZ53G,2XXW0J4N117Q,1335478101000,/product/FPR477BM,00:08:21,http://www.google.com,74,6
X6DUSPR2R53VZ53G,2XXW0J4N117Q,1335478101000,/addToCart/FPR477BM,00:09:35,http://www.google.com,0,0
C142FL33KKCV603E,UJAQ1TQWAGVL,1335478185000,/product/7Y4FP655,00:09:45,http://www.twitter.com,0,0
```

The example shows how this distributed in-memory dataset is queried by a SQL statement with just a few lines of code. The statement may be of the form: `select * from pages where rating > 1`.
```
def fromPages(sc:SparkContext,source:RDD[LogPage],query:String):SchemaRDD = {
    
  val sqlc = new SQLContext(sc)
  val schema = sqlc.createSchemaRDD(source)
    
  val pages = sqlc.registerRDDAsTable(schema, "pages")

  sqlc.sql(query)    
    
}
```

Checkout abandonment is of interest to any online retailer. We show how to determine abandoned checkouts from W3C web server logs with just a few lines of Scala code by using the functionality of Spark and Spark SQL.

In this project, a conversion goal is defined as a sequence of selected page urls the website of an online retailer. From this definition, web visits may be categorized as follows:

| category | description |
| --- | --- |
| 0 | A visitor has not visited any of the predefined pages |
| 1 | A visitor has visited at least one of the pages |
| 2 | A visitor has visited all pages in the predefined order |

Following this model, checkout abandonment means to discover all web visits from the W3C server log that are categorized with `category = 1`.

The subsequent lines of Scala code describe how to categorize web visits:
```
/* Group source by sessionid */
val dataset = source.groupBy(group => group._1)
dataset.map(valu => {
  
  /* Sort single session data by timestamp */
  val data = valu._2.toList.sortBy(_._2)

  val pages = data.map(_._4)
 
  /* Total number of page clicks */
  val total = pages.size
  
  val (sessid,starttime,userid,pageurl,visittime,referrer) = data.head
  val endtime = data.last._2
  
  /* Total time spent for session */
  val timespent = (if (total > 1) (endtime - starttime) / 1000 else 0)
  val exitpage = pages(total - 1)
      
  val category = categorize(pages)      
  new LogFlow(sessid,userid,total,starttime,timespent,referrer,exitpage,category)
      
})

```
These categorized web sessions may then be evaluated by the `select * from flows where category = 1':
```
def fromFlows(sc:SparkContext,source:RDD[LogFlow],query:String):SchemaRDD = {
    
  val sqlc = new SQLContext(sc)
  val schema = sqlc.createSchemaRDD(source)
    
  val pages = sqlc.registerRDDAsTable(schema, "flows")

  sqlc.sql(query)    
    
}

```

---

### Predictions from Web Logs

Visitor conversion is the main topic for any ecommerce website, as conversion (usually) means purchasing a product or service. Conversion could also mean that the visitors takes a certain action, such as registrating for an email news letter or any other action the retailer benefits from.

Predictive Analytics is a means to facilitate higher conversion rates: 

Based on historical customer engagement data, and equipped with the visitor's behavior in any ongoing session, appropriate techniques enable to determine the likelihood of the visitor converting during the session. From this information further actions may then be triggered to enhance the chances of conversion.

We show how to predict the likelihood of a visitor converting in an ongoing session by using the functionality Spark and a Predictive Model implemented in Scala.

The predictor provided in this project is a `BayesianPredictor` as it is based on the [Bayesian Discriminant Analysis](http://en.wikipedia.org/wiki/Bayesian_inference). To build the predictor, the following information is extracted from the web log data:

1. Click count histogram for unconverted visitors
2. Click count histogram for converted visitors
3. Session count where visitors did not convert
4. Session count where visitors did convert

The Scala code below is taken from the `BayesianPredictor` object and shows how to derive the histograms mentioned:
```
/**
 * Input = (sessid,userid,total,starttime,timespent,referrer,exiturl,flowstatus)
 */
private def histogram(dataset:RDD[(String,String,Int,Long,Long,String,String,Int)]):RDD[((Int,Int),Int)] = {
  /*
   * The input contains one row per session. Each row contains the number of clicks 
   * in the session, time spent in the session and a boolean indicating whether the 
   * user converted during the session.
   */
  val histogram = dataset.map(valu => {
      
    val clicksPerSession = valu._3
    val userConvertedPerSession = if (valu._8 == FLOW_COMPLETED) 1 else 0
      
    val k = (clicksPerSession,userConvertedPerSession)
    val v = 1
      
    (k,v)
    
  }).reduceByKey(_ + _)
    
  /*
   * Each row of the output contains the conversion flag, click count 
   * per session and the number of sessions with those click counts. 
   */ 
  histogram
    
}
```

From this information, the `BayesianPredictor` computes the following parameters:

1. p(c|v=0) : Probability of clicks per session, given the visitor did not convert in the session 
2. p(c|v=1) : Probability of clicks per session, given the visitor converted in the session
3. p(v=0) : Unconditional probability of visitor did not convert in a session
4. p(v=1) : Unconditional probability of visitor converted in a session

Using Baye's theory, the probability of a visitor converting, given the clicks in the session `p(v=1|c)` may be computed by the following formula:
```
p(v=1|c) = p(c|v=1) * p(v=1) / (p(c|v=0) * p(v=0) + p(c|v=1) * p(v=1))
```
The `BayesianModel` finally calculates the probability of visitor conversion from the number of clicks in a session. 


