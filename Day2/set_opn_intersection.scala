// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120825/2709359236233034/latest.html

val augData = sc.textFile("/FileStore/tables/nasa_august.tsv")

val julyData = sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val augRdd = augData.map(x => x.split("\t")(0) )

val julyRdd = julyData.map(x => x.split("\t")(0) ) 

val intersectionRdd = julyRdd.intersection(augRdd)

intersectionRdd.collect()

// COMMAND ----------

val finalRdd = intersectionRdd.filter(x => !(x.contains("host")))

finalRdd.collect()

// COMMAND ----------

finalRdd.count()

// COMMAND ----------


