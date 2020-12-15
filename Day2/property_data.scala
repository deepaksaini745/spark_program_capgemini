// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/4088012569517867/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeHeader = data.filter( line => !line.contains("Price"))

removeHeader.take(10)


// COMMAND ----------

val roomRdd = removeHeader.map(x =>  ( x.split(",")(3).toInt , (1, x.split(",")(2).toDouble)  ) )

roomRdd.collect()

// COMMAND ----------

val reducedRDD = roomRdd.reduceByKey( (x,y) => ( x._1+y._1, x._2+y._2 ))

reducedRDD.take(10)

// COMMAND ----------

val finalRdd = reducedRDD.mapValues( x => x._2/x._1 )

finalRdd.collect()

// COMMAND ----------

for( (bedroom, avg) <- finalRdd.collect() ) println(bedroom + " : " + avg)

// COMMAND ----------

finalRdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


