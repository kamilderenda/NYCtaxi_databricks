# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_months_ago_start=date.today().replace(day=1)-relativedelta(months=3)

# COMMAND ----------

df=spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime>'{two_months_ago_start}'")

# COMMAND ----------

df=df.groupBy(df.tpep_pickup_datetime.cast('date').alias('pickup_date')).\
    agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg('passenger_count'),1).alias('avg_passenger_count'),
        F.round(F.avg('trip_distance'),1).alias('avg_trip_distance'),
        F.round(F.avg('fare_amount'),2).alias('avg_fare_amount'),
        F.max('fare_amount').alias('max_fare_amount'),
        F.min('fare_amount').alias('min_fare_amount'),
        F.round(F.sum('total_amount'),2).alias('total_revenue')
    )

# COMMAND ----------

df.write.mode('append').saveAsTable('nyctaxi.03_gold.daily_trips_summary')

# COMMAND ----------

