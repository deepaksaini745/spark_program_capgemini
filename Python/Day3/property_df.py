# Databricks notebook source
# /FileStore/tables/Property_data.csv

# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/927397281093190/2709359236233034/latest.html

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, mean, max, min

# COMMAND ----------

spark = SparkSession.builder.appName("PropertyDataExample").getOrCreate()

# COMMAND ----------

pdf = spark.read.csv("/FileStore/tables/Property_data.csv", header=True, inferSchema=True)

#pdf.count()
pdf.show()

# COMMAND ----------

# DBTITLE 1,Average price for each bedroom type
avg = pdf.groupBy("Bedrooms").avg("price").alias("Average_Price")

avg.show()

# COMMAND ----------

# DBTITLE 1,Total flats in a particular location
total_flats = pdf.groupBy("location").count()

total_flats.show()

# COMMAND ----------

# DBTITLE 1,Data of flats with (price<90k)
df1 = pdf.filter( pdf["price"] < 90000 ).select("*")

df1.count()

# COMMAND ----------

df1.groupBy("Bedrooms").avg("Price").show()

# COMMAND ----------

# DBTITLE 1,Data of flats with (price>90k and price<150k)
df2 = pdf.filter( (pdf["price"] > 90000) & (pdf["price"] < 150000) ).select("*")

df2.count()

# COMMAND ----------

df2.groupBy("Bedrooms").avg("Price").show()

# COMMAND ----------

# DBTITLE 1,Data of flats with (price>150k)
df3 = pdf.filter( pdf["price"] > 150000 ).select("*")

df3.count()

# COMMAND ----------

df3.groupBy("Bedrooms").avg("Price").show()

# COMMAND ----------

# DBTITLE 1,Renaming column
pdf = pdf.withColumnRenamed("Price SQ Ft", "Price_Sq_Ft")

pdf.show()

# COMMAND ----------

# DBTITLE 1,Maximum Price of whole data
pdf.select( max(pdf["price"]) ).show()

# COMMAND ----------

# DBTITLE 1,Minimum Price of whole data
pdf.select( min(pdf["price"]) ).show()

# COMMAND ----------

# DBTITLE 1,Average Price of whole data
pdf.select( mean(pdf["price"]) ).show()

# COMMAND ----------

# DBTITLE 1,Price Per Bedroom
ppbed = pdf.withColumn( "Price_Per_Bedroom", format_number(((pdf["Price_Sq_Ft"]*pdf["Size"])/(pdf["Bedrooms"])), 2) )

ppbed.show()

# COMMAND ----------

# DBTITLE 1,Price Per Bathroom
ppbath = ppbed.withColumn( "Price_Per_Bathroom", format_number( ((ppbed["Price_Sq_Ft"]*ppbed["Size"])/(ppbed["Bathrooms"])),2 ) )

ppbath.show()

# COMMAND ----------


