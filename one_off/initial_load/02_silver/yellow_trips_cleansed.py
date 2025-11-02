# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_monthss_ago_start=date.toda().replace(day=1)-relativedelta(months=3)
one_monthss_ago_start=date.today().replace(day=1)-relativedelta(months=2)

# COMMAND ----------

df=spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").filter(f"tpep_pickup_datetime >= '{two_monthss_ago_start}' AND tpep_pickup_datetime <= '{one_monthss_ago_start}'")

# COMMAND ----------

df=df.select(
     F.when(F.col('VendorID')==1, "Creative Mobile Technologies, LLC")
         .when(F.col('VendorID')==2, "Curb Mobility, LLC")
         .when(F.col('VendorID')==6, "Myle Technologies Inc")
         .when(F.col('VendorID')==7, "Helix")\
         .otherwise('other').alias('vendor'),
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    F.timestamp_diff('MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias('trip_duration'),
    "passenger_count",
    "trip_distance",
    F.when(F.col('RatecodeID')==1, "Standard rate")\
         .when(F.col('RatecodeID')==2, "JFK")\
         .when(F.col('RatecodeID')==3, "Newark")\
         .when(F.col('RatecodeID')==4, "Nassau or Westchester")\
         .when(F.col('RatecodeID')==5, "Negotiated fare")\
         .when(F.col('RatecodeID')==6, "Group ride")\
         .otherwise('Unknown').alias('rate'),
    "store_and_fwd_flag",
    F.col('PULocationID').alias('pickup_location_id'),
    F.col('DOLocationID').alias('dropoff_location_id'),
    F.when(F.col('payment_type')==0, "Flex Fare Trip")\
         .when(F.col('payment_type')==1, "Credit card")\
         .when(F.col('payment_type')==2, "Cash")\
         .when(F.col('payment_type')==3, "No charge")\
         .when(F.col('payment_type')==4, "Dispute")\
         .when(F.col('payment_type')==6, "Voided trip")\
         .otherwise('Unknown').alias('payment_type'),
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    F.col("Airport_fee").alias("airport_fee"),
    F.col("cbd_congestion_fee").alias("cbd_congestion_fee"),
    'processed_timestamp'
)


# COMMAND ----------

df.write.mode('append').saveAsTable('nyctaxi.02_silver.yellow_trips_cleansed')