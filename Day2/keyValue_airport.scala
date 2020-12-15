// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120832/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val dataRdd = data.map(line => ( line.split(",")(1), line.split(",")(3) ))

dataRdd.take(3)

// COMMAND ----------

// DBTITLE 1,airport which is not in canada
//Tuple <= dictionary

val NotCanada = dataRdd.filter( x => x._2 != "\"Canada\"")
NotCanada.take(2)

// COMMAND ----------

NotCanada.filter(x => x._2 == "\"Canada\"").take(2)

// COMMAND ----------

val listData = List("Deepak 2020","Nancy 1998", "Abhinav 1997", "Kartik 2100")

// COMMAND ----------

val kvrdd = sc.parallelize(listData)

val rddnew = kvrdd.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))

rddnew.take(3)




// COMMAND ----------

rddnew.mapValues(x => x+10).take(3)

// COMMAND ----------

val data2 = data.map(line => ( line.split(",")(1), line.split(",")(11) ))

data2.take(5)

// COMMAND ----------

data2.mapValues(x => x.toLowerCase).take(5)

// COMMAND ----------


