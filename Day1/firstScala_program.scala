// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/2952220317321676/2709359236233034/latest.html

var a:Int = 1

var b = 1:Int



a = 10

// COMMAND ----------

lazy val a = 10

a

// COMMAND ----------

lazy val variable_lazy = {println("hello world");5}

variable_lazy


// COMMAND ----------

var new2 = Array(1,2,3,4,5,6)


// COMMAND ----------

// DBTITLE 1,First RDD - SC(Spark Context -> Object in databricks which is used to work with a cluster)
val data = List(1,2,3,4,5)

//parallelize method used to create rdd and is lazy in nature
val creationRDD = sc.parallelize(data)

// COMMAND ----------

//to get result of rdd -> action on your rdd

creationRDD.collect()

// COMMAND ----------

//get total partitions for your data

creationRDD.partitions.size

// COMMAND ----------

val rddPartition = sc.parallelize(List(1,2,3,4),2 )

// COMMAND ----------

rddPartition.partitions.size

// COMMAND ----------

//count is also action -> return the number of elements

rddPartition.count()

// COMMAND ----------

//map -> transformation

//we are using map to create new rdd from an existing rdd [rddPartitions]
val maprdd = rddPartition.map( x => x*x*x )

//maprdd.take(3)

maprdd.collect()

// COMMAND ----------

maprdd.filter( x => x % 2 == 0 ).collect()

// COMMAND ----------

val mainrdd = sc.parallelize(List("hey","hello","deepak","bro"))

mainrdd.collect()

// COMMAND ----------

// map vs flatmap

mainrdd.map( x => x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap( x => x.split(",")).collect()

// COMMAND ----------

//creating rdd for key value pair

val rdd0 = sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd = rdd0.map( x => (x,1))

keyrdd.collect()

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()

// COMMAND ----------


