# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df=spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_source/nyctaxi_yellow/*")

# COMMAND ----------

df=df.withColumn("processed_timestamp", F.current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")