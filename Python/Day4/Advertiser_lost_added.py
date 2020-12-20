# Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/474020937699520/2709359236233034/latest.html

from pyspark.sql import SparkSession

# COMMAND ----------

# /FileStore/tables/AdvertiserLostAdded.csv

spark = SparkSession.builder.appName("AdvertiserUsecase").getOrCreate()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/AdvertiserLostAdded.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.registerTempTable('data')

# COMMAND ----------

sqlContext.sql('select * from data').show()

# COMMAND ----------

clr18 = df.select('channel','advertiser').filter((df['channel']=='color') & (df['year']==2018))
clr18.show()


sp18 = df.select('channel','advertiser').filter((df['channel']=='star-plus') & (df['year'] == 2018))
sp18.show()


clr19 = df.select('channel','advertiser').filter((df['channel']=='color') & (df['year']==2019))
clr19.show()


sp19 = df.select('channel','advertiser').filter((df['channel']=='star-plus') & (df['year'] == 2019))
sp19.show()

# COMMAND ----------

clrLost19 = clr18.exceptAll(clr19).withColumnRenamed('advertiser','lost advertiser')
clrNew19  = clr19.exceptAll(clr18).withColumnRenamed('advertiser','new advertiser')
spLost19 = sp18.exceptAll(sp19).withColumnRenamed('advertiser','lost advertiser')
spNew19  = sp19.exceptAll(sp18).withColumnRenamed('advertiser','new advertiser')
spLost19.show()
spNew19.show()
clrLost19.show()
clrNew19.show()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType

AdvertiserSchema = StructType([StructField('channel',StringType(),True),
                              StructField('advertiserLost',StringType(),True),
                              StructField('advertiserAdded',StringType(),True)])

# COMMAND ----------

from pyspark.sql.functions import lit
final1 = spLost19.join(spNew19,'channel','leftouter')
final2 = clrLost19.join(clrNew19,'channel','inner')
dfLostAdded = final1.union(final2).withColumn('year',lit(2019))

# COMMAND ----------

dfLostAdded.select('year','channel','lost advertiser','new advertiser').show()

# COMMAND ----------


