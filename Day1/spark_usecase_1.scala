// Databricks notebook source
// /FileStore/tables/ratings.csv

// creating an rdd now from external datasets

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/1139243164332981/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsData = data.map(x => x.split(",")(2))

ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue()
