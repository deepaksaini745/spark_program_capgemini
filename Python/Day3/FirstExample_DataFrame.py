# Databricks notebook source
# DBTITLE 1,Import SparkSession
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/927397281093166/2709359236233034/latest.html

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("FirstExample17Dec").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create DataFrame
# /FileStore/tables/FriendsData.csv

#creating dataframe
dfStart = spark.read.csv("/FileStore/tables/FriendsData.csv", header = True, inferSchema = True)

#inferSchema will take the datatype as the data is and not as string

# COMMAND ----------

# DBTITLE 1,Display Data
#display the data
dfStart.show()

# COMMAND ----------

# DBTITLE 1,Get schema
dfStart.printSchema()

# COMMAND ----------

# DBTITLE 1,Describe --> Aggregated Value for whole dataset
dfStart.describe().show()

# COMMAND ----------

dfStart.collect()

# COMMAND ----------

# DBTITLE 1,get column names
dfStart.columns

# COMMAND ----------

# DBTITLE 1,Print schema
dfStart.schema

# COMMAND ----------

# DBTITLE 1,Print selected columns(select)
#see only specific columns

df0 = dfStart.select("id","FrIeNdS","NAME")

df0.show()

# COMMAND ----------

print(type(df0))

# COMMAND ----------

# DBTITLE 1,Rename Column
dfStart.withColumnRenamed("friends", "NEW Friends").show()

# COMMAND ----------

# DBTITLE 1,Filter
dfStart.filter(dfStart["Age"] > 50).select(dfStart["Age"],dfStart["name"] ).show()

# COMMAND ----------

# DBTITLE 1,name of people with age>50 and friends>100
dfStart.filter((dfStart["Age"] > 50) & (dfStart["Friends"] > 100)).select(dfStart["name"] ).show()

# COMMAND ----------

# DBTITLE 1,Create new col using older col
dfStart.withColumn("New Age", dfStart["Age"]+10).show() #use same col name to put the data into same column without creating a new column

# COMMAND ----------

# DBTITLE 1,Alias col name
dfStart.withColumn("Age", dfStart["Age"]+10)\
.select("*").show()
#.select(dfStart["Name"].alias("newName")).show()

# COMMAND ----------

#dfStart = dfStart.filter(dfStart["Id"]%2 == 0).withColumn( "Total Amount", ( dfStart["Friends"]*10*dfStart["Age"] )/12 )

#TotalAmountDF = dfStart.select(dfStart["Name"],dfStart["Age"],dfStart["Friends"].alias("Total Friends"),dfStart["Total Amount"] )

#TotalAmountDF.show()

# COMMAND ----------

from pyspark.sql.functions import format_number, round, mean, max, min

# COMMAND ----------

dfStart.select("*",format_number( dfStart['Friends']/dfStart['Age'] , 2).alias("friend_by_age") ).show()

# COMMAND ----------

dfStart.select( min( dfStart["age"] ) ).show()

# COMMAND ----------

dfStart.columns

# COMMAND ----------

dfStart.groupBy("age").max("friends").show()
