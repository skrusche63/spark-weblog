![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

### Web Log Mining with Spark

Web Log Analysis is based on log files that are compliant to the W3C web server log format. The IIS is a prominant candidate that supports this format. The mining functionality itself is not restricted to this format and may be easily adapted to other log files by changing configuration parameters.

Web Log Mining is used here to extract common web analytic parameters.

---

### Insights from Web Logs

Checkout abandonment is of interest to any online retailer. We show how to determine abandoned checkouts from W3C web server logs with just a few lines of Scala code by using the functionality of Spark and Spark SQL.

The code example illustrates how a distributed in-memory data structure (RDD) may be queried a SQL statement (query).
```
/**
 * Apply query to in-memory web page descriptions
 */
def fromPages(sc:SparkContext,source:RDD[LogPage],query:String):SchemaRDD = {
    
  val sqlc = new SQLContext(sc)
  val schema = sqlc.createSchemaRDD(source)
    
  val pages = sqlc.registerRDDAsTable(schema, "pages")

  sqlc.sql(query)    
    
}
```


---

### Predictions from Web Logs

Visitor conversion is the main topic for any ecommerce website, as conversion (usually) means purchasing a product or service. Conversion could also mean that the visitors takes a certain action, such as registrating for an email news letter or any other action the retailer benefits from.

Predictive Analytics is a means to facilitate higher conversion rates: 

Based on historical customer engagement data, and equipped with the visitor's behavior in any ongoing session, appropriate techniques enable to determine the likelihood of the visitor converting during the session. From this information further actions may then be triggered to enhance the chances of conversion.

We show how to predict the likelihood of a visitor converting in an ongoing session by using the functionality Spark and a Predictive Model implemented in Scala.
