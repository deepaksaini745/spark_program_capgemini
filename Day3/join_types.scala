// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120832/2709359236233034/latest.html

val data1 = sc.parallelize(List( ("deepak", 2020), ("Saini", 2022) ))

val data2 = sc.parallelize(List( ("nancy", "tyagi"), ("Saini", "US") ))

// COMMAND ----------

data1.join(data2).collect()

// COMMAND ----------

data1.leftOuterJoin(data2).collect()

// COMMAND ----------


