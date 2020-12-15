// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120832/2709359236233034/latest.html

val data = List("deepak", "saini", "deepak", "sir", "saini", "capg")

// COMMAND ----------

val datardd = sc.parallelize(data)

// COMMAND ----------

datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val data2 = List(1,2,3,4,5)

val data2rdd = sc.parallelize(data2)

// COMMAND ----------

val productRDD = data2rdd.reduce( (x,y) => x*y )

productRDD

// COMMAND ----------


