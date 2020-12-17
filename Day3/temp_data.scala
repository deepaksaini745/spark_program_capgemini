// Databricks notebook source
// /FileStore/tables/Airport_data_csv___Airport_data_csv.csv
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/3869542895595330/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/Airport_data_csv___Airport_data_csv.csv")

// COMMAND ----------

val tempData = data.map( x => ( x.split(",")(0) ,  x.split(",")(2).toInt ) )

tempData.take(10)

// COMMAND ----------

// DBTITLE 1,Maximum Temperature
val maxTemp = tempData.reduceByKey(math.max(_,_))
maxTemp.collect()

// COMMAND ----------

// DBTITLE 1,Minimum Temperature
val minTemp = tempData.reduceByKey(math.min(_,_))
minTemp.collect()

// COMMAND ----------

// DBTITLE 1,Average Temperature
val avgData = data.map( x => ( x.split(",")(0), (1, x.split(",")(2).toInt) ) )

avgData.take(10)

// COMMAND ----------

val newData = avgData.reduceByKey((x,y)=> (x._1+y._1, x._2+y._2))

newData.collect()

// COMMAND ----------

val finalData = newData.mapValues( x=> x._2/x._1 )

finalData.collect()

// COMMAND ----------

// DBTITLE 1,Airport ----> Average Temperature
for( (ap,temp) <- finalData.collect() ) println(ap +" ----> "+ temp)

// COMMAND ----------

// DBTITLE 1,Airport wise data count
val countData = data.map( x =>  x.split(",")(0) )

countData.countByValue()
