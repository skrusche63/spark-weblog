![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

### Web Log Mining with Spark

Web Log Analysis is based on log files that are compliant to the W3C web server log format. The IIS is a prominant candidate that supports this format. The mining functionality itself is not restricted to this format and may be easily adapted to other log files by changing configuration parameters.

Web Log Mining is used here to extract common web analytic parameters.

---

### Insights from Web Logs

Spark and Spark SQL are big data (mining) tools, that make it very easy to retrieve fasts insights from large-scale datasets. The following example describes how mining results, represented as Spark compliant data structures, are further evaluated by applying SQL queries. 

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


---

### Predictions from Web Logs

Visitor conversion is the main topic for any ecommerce website, as conversion (usually) means purchasing a product or service. Conversion could also mean that the visitors takes a certain action, such as registrating for an email news letter or any other action the retailer benefits from.

Predictive Analytics is a means to facilitate higher conversion rates: 

Based on historical customer engagement data, and equipped with the visitor's behavior in any ongoing session, appropriate techniques enable to determine the likelihood of the visitor converting during the session. From this information further actions may then be triggered to enhance the chances of conversion.

We show how to predict the likelihood of a visitor converting in an ongoing session by using the functionality Spark and a Predictive Model implemented in Scala.
