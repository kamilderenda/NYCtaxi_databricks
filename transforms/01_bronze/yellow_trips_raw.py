# Databricks notebook source
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from datetime import datetime

# COMMAND ----------

two_months_ago = (datetime.now() - relativedelta(months=3)).strftime("%Y-%m")

# COMMAND ----------

df=spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_source/nyctaxi_yellow/{two_months_ago}")

# COMMAND ----------

df=df.withColumn("processed_timestamp", F.current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")