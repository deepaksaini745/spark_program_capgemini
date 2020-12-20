# Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5809874484629813/3055977344712233/2709359236233034/latest.html

import urllib

ACCESS_KEY = "AKIA4WR7LEUPDH5NNOXG"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
 
# bucket name
 
AWS_BUCKET_NAME = "firedataregex"
MOUNT_NAME = "deepak"
 
dbutils.fs.mount("s3n://%s:%s@%s" %  (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" %  MOUNT_NAME )



# COMMAND ----------

display(dbutils.fs.ls("/mnt/deepak"))

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/deepak/Fire_Department_Calls_for_Service.csv", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show( )

# COMMAND ----------

df.columns

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType 

# COMMAND ----------

""" 'Call Number',
 'Unit ID',
 'Incident Number',
 'Call Type',
 'Call Date',
 'Watch Date',
 'Received DtTm',
 'Entry DtTm',
 'Dispatch DtTm',
 'Response DtTm',
 'On Scene DtTm',
 'Transport DtTm',
 'Hospital DtTm',
 'Call Final Disposition',
 'Available DtTm',
 'Address',
 'City',
 'Zipcode of Incident',
 'Battalion',
 'Station Area',
 'Box',
 'Original Priority',
 'Priority',
 'Final Priority',
 'ALS Unit',
 'Call Type Group',
 'Number of Alarms',
 'Unit Type',
 'Unit sequence in call dispatch',
 'Fire Prevention District',
 'Supervisor District',
 'Neighborhooods - Analysis Boundaries',
 'Location',
 'RowID',
 'shape',
 'Supervisor Districts',
 'Fire Prevention Districts',
 'Current Police Districts',
 'Neighborhoods - Analysis Boundaries',
 'Zip Codes',
 'Neighborhoods (old)',
 'Police Districts',
 'Civic Center Harm Reduction Project Boundary',
 'HSOC Zones',
 'Central Market/Tenderloin Boundary Polygon - Updated',
 'Neighborhoods',
 'SF Find Neighborhoods',
 'Current Police Districts 2',
 'Current Supervisor Districts' """



# COMMAND ----------

fireSchema = StructType( [ (StructField('CallNumber', IntegerType(), True)),
                         (StructField('UnitId', StringType(), True)),
                         (StructField('Incident Number', IntegerType(), True)),
                         (StructField('CallType', StringType(), True)),
                         (StructField('CallDate', StringType(), True)),
                         (StructField('WatchDate', StringType(), True )),                                   
                         (StructField('ReceivedDTM', StringType(), True)),
                         (StructField('EntryDTM', StringType(), True)),
                         (StructField('DispatchDTM', StringType(), True)),
                         (StructField('ResponseDTM', StringType(), True)),
                         (StructField('OnSceneDTM', StringType(), True)),
                         (StructField('TransportDTM', StringType(), True)),
                         (StructField('HospitalDTM', StringType(), True)),
                         (StructField('CallFinalDisposition', StringType(), True)),
                         (StructField('AvailableDTM', StringType(), True)),
                         (StructField('Address', StringType(), True)),
                         (StructField('City', StringType(), True)),
                         (StructField('ZipCodeOfIncident', IntegerType(), True)),
                         (StructField('Battalion', StringType(), True)),
                         (StructField('StationArea', StringType(), True)),
                         (StructField('Box', StringType(), True)),
                         (StructField('OriginalPriority', StringType(), True)),
                         (StructField('Priority', StringType(), True)),
                         (StructField('FinalPriority', IntegerType(), True)),
                         (StructField('ALSUnit', BooleanType(), True)),
                         (StructField('CallTypeGroup', StringType(), True)),
                         (StructField('NumberOfAlarms', IntegerType(), True)),
                         (StructField('UnitType', StringType(), True)),
                         (StructField('UnitSequenceInCallDispatch', IntegerType(), True)),
                         (StructField('FirePreventionDistrict', StringType(), True)),
                         (StructField('SupervisorDistrict', StringType(), True)),
                         (StructField('NeighborhooodsAnalysisBoundaries', StringType(), True)),
                         (StructField('Location', StringType(), True)),
                         (StructField('RowID', StringType(), True)),
                         (StructField('Shape', StringType(), True)),
                         (StructField('SupervisorDistricts', IntegerType(), True)),
                         (StructField('FirePreventionDistricts', IntegerType(), True)),
                         (StructField('CurrentPoliceDistricts', IntegerType(), True)),
                         (StructField('NeighborhoodsAnalysisBoundaries', IntegerType(), True)),
                         (StructField('ZipCodes', IntegerType(), True)),
                         (StructField('Neighborhoods(old)', IntegerType(), True)),
                         (StructField('PoliceDistricts', IntegerType(), True)),
                         (StructField('CivicCenterHarmReductionProjectBoundary', IntegerType(), True)),
                         (StructField('HSOCZones', IntegerType(), True)),
                         (StructField('CentralMarket', IntegerType(), True)),
                         (StructField('Neighborhoods', IntegerType(), True)),
                         (StructField('SFFindNeighborhoods', IntegerType(), True)),
                         (StructField('CurrentPoliceDistricts2', IntegerType(), True)),
                         (StructField('CurrentSupervisorDistricts', IntegerType(), True))] )

# COMMAND ----------

dfNew = spark.read.csv("dbfs:/mnt/deepak/Fire_Department_Calls_for_Service.csv", header = True, schema = fireSchema)

# COMMAND ----------

display(dfNew.limit(2))

# COMMAND ----------

dfNew.count()

# COMMAND ----------

dfNew.select("CallType").distinct().show()

# COMMAND ----------

# MAGIC %md if you want to show proper output, use FALSE

# COMMAND ----------

dfNew.select("CallType").distinct().show(20, False)

# COMMAND ----------

# MAGIC %md ***TIP*** : always select those columns which are of our need, dont choose other column for groupBy

# COMMAND ----------

display( dfNew.select("CallType").groupBy("CallType").count().orderBy("count", ascending=False) )

# COMMAND ----------

# MAGIC %md we are importing unix timestamp for converting string to date

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

# COMMAND ----------

pattern1 = 'MM/dd/yyyy'

pattern2 = 'MM/dd/yyyy hh:mm:ss a'

#to_pattern1 = 'yyyy-MM-dd'

display( dfNew.withColumn( "NewCallDate", unix_timestamp(dfNew["CallDate"], pattern1).cast("timestamp") ).drop("CallDate")\
        .withColumn( "NewWatchDate", unix_timestamp(dfNew["WatchDate"], pattern1).cast("timestamp") ).drop("WatchDate")\
        .withColumn( "NewReceivedDTM", unix_timestamp(dfNew["ReceivedDTM"], pattern2).cast("timestamp") ).drop("ReceivedDTM")\
       .withColumn( "NewDispatchDTM", unix_timestamp(dfNew["DispatchDTM"], pattern2).cast("timestamp") ).drop("DispatchDTM")\
       .withColumn( "NewResponseDTM", unix_timestamp(dfNew["ResponseDTM"], pattern2).cast("timestamp") ).drop("ResponseDTM")\
        .withColumn( "NewOnSceneDTM", unix_timestamp(dfNew["OnSceneDTM"], pattern2).cast("timestamp") ).drop("OnSceneDTM")\
        .withColumn( "NewTransportDTM", unix_timestamp(dfNew["TransportDTM"], pattern2).cast("timestamp") ).drop("TransportDTM")\
        .withColumn( "NewHospitalDTM", unix_timestamp(dfNew["HospitalDTM"], pattern2).cast("timestamp") ).drop("HospitalDTM")\
        .withColumn( "NewAvailableDTM", unix_timestamp(dfNew["AvailableDTM"], pattern2).cast("timestamp") ).drop("AvailableDTM")  )

# COMMAND ----------


