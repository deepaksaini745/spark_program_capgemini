// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/167256236120840/2709359236233034/latest.html

val data = sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

def pno(i:Int): Boolean = {
  if (i <= 1)
        false
    else if (i == 2)
        true
    else
        !(2 until i).exists(n => i % n == 0)
}


// COMMAND ----------

val header = data.first()

val data2 = data.filter(row => row != header)

val data3 = data2.map(x => x.toInt)

val res = data3.filter(x => pno(x))

res.take(3)

// COMMAND ----------

val productRDD = res.reduce( (x,y) => x+y )

// COMMAND ----------


