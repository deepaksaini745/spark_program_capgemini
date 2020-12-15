// Databricks notebook source
// /FileStore/tables/FriendsData.csv
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/4088012569517876/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

data.take(10)

// COMMAND ----------

// DBTITLE 1,Removing Header
val remHead = data.filter(x => !(x.contains("name")) )

remHead.take(10)

// COMMAND ----------

val frndRdd = remHead.map( x => ( x.split(",")(2).toInt , (1, x.split(",")(3).toInt) ) )

frndRdd.take(10)

// COMMAND ----------

val sumRdd = frndRdd.reduceByKey( (x,y) => (x._1+y._1, x._2+y._2) )

sumRdd.collect()

// COMMAND ----------

val avgRdd = sumRdd.mapValues(x => x._2/x._1)

avgRdd.collect() 

// COMMAND ----------

// DBTITLE 1,Age ----> Average Number Of Friends (Task1)
for( (age,friends) <- avgRdd.collect())  println(age+" ----> "+friends)

// COMMAND ----------

// DBTITLE 1,Max no of friends for each age(Task2)
val newRdd = remHead.map( x => ( x.split(",")(2).toInt ,  x.split(",")(3).toInt ) )

newRdd.take(10)

// COMMAND ----------

val maxVal = newRdd.reduceByKey(math.max(_,_))
maxVal.collect()

// COMMAND ----------

// DBTITLE 1,Age ----> Max_Number_Of_Friends
for( (age,maxFrnd) <- maxVal.collect())  println(age+" ----> "+maxFrnd)

// COMMAND ----------


