// Databricks notebook source
// /FileStore/tables/StackOverflow_survey_responses.csv

// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/554456616779812/2709359236233034/latest.html

val dataRdd = sc.textFile("/FileStore/tables/StackOverflow_survey_responses.csv")

// COMMAND ----------

// DBTITLE 1,Accumulator Variable
val total_rows = sc.longAccumulator

val midMissingSalary = sc.longAccumulator

// COMMAND ----------

//put logic in rdd

val afghanistanRdd = dataRdd.filter(responseData => {
  val splitData = responseData.split(",", 100) 
  
  total_rows.add(1)
  
  if( splitData(14).isEmpty ){
    midMissingSalary.add(1)
  }
  
  splitData(2) == "Afghanistan"
  
} )

// COMMAND ----------

print(total_rows.value)

// COMMAND ----------

afghanistanRdd.take(3)

// COMMAND ----------

print(total_rows.value)

// COMMAND ----------


