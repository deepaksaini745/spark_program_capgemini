# Databricks notebook source
#https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/1409905252664496/2709359236233034/latest.html

a=10
b=10
c=10

print(id(a), id(b), id(c))

c=c+1

b=c

print(id(a), id(b), id(c))
del c 

print(id(a), id(b))


# COMMAND ----------

s1 = 'INFORMATION'

# COMMAND ----------

print("deepak" or 20)

print( 0 or "deepak")

print( 0 or 0)

print("" or "")

# COMMAND ----------

print("tushar" and 0)

# COMMAND ----------

# DBTITLE 1,ternary operator
a = b = 20

print(a)

data = a if a != 20 else "Not found value less than 20"

print(data)

# COMMAND ----------

# string [Slicing]

# -> index position

s1 = "REGEX"


s2 = "Hello world, Deepak this side !!!"

s2[0:]

s2[:11]

# step
s3="INFORMATION"
# ------------------------All below works same
s3[::]

s3[::1]

s3[:]
# --------------------------------------

# COMMAND ----------

s3[2:10:2]

# COMMAND ----------

print("Negative index output")
s3[-9:-1:2]

# COMMAND ----------

s3[-1: -12: -3]

# to reverse string

s3[::-1]

# COMMAND ----------

data = "NAYAN"

# verify palindrome -----------------------

"Palindrome" if data == data[::-1] else "Not Palindrome"

# COMMAND ----------


