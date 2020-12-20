# Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/3055977344712224/2709359236233034/latest.html

df = spark.read.csv("/FileStore/tables/Diabetes_datasets.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

display(df.dropna())

# COMMAND ----------

#display( df.dropna( thresh=2 ) )

#display( df.dropna( subset="Diabetes" ) )

#display( df.dropna( how="all" ) )

#display( df.dropna( subset=["Diabetes", "Age"] ) )

df.drop("Insulin").show()

# COMMAND ----------

display(df.fillna(0, subset="Diabetes"))

