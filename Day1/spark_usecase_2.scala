// Databricks notebook source
// /FileStore/tables/airports.text

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/3695146590532606/2709359236233034/latest.html

val apdata = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val USdata = apdata.filter( line => line.split(",")(3) == "\"United States\"" )

USdata.take(2)

// COMMAND ----------

def splitIP(line:String) = {
  
  val datasplit = line.split(",") 
  
  val apname = datasplit(1)
  
  val apcity = datasplit(2)
  
  (apname, apcity)
}

// COMMAND ----------

val reqdata = USdata.map(splitIP)

reqdata.take(2)

// COMMAND ----------

// DBTITLE 1,Second Approach
USdata.map( line => {
  val splitdata = line.split(",")
  splitdata(1) + " -- " + splitdata(2) }).take(10)
