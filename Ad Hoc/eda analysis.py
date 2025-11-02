# Databricks notebook source
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

df=spark.read.table('nyctaxi.02_silver.yellow_trips_enriched')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Vendor which makes the most revenue

# COMMAND ----------

df.groupBy('Vendor').agg(F.sum(F.col('total_amount'))).orderBy(F.col('sum(total_amount)').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # The most popular pickup borough

# COMMAND ----------

df.groupBy('pickup_borough').agg(F.count(F.col('total_amount'))).orderBy(F.col('count(total_amount)').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # The most common journey

# COMMAND ----------

df.groupBy('pickup_borough','dropoff_borough').agg(F.count(F.col('total_amount'))).orderBy(F.col('count(total_amount)').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total number of trips and revenue per day

# COMMAND ----------

df2=spark.read.table("nyctaxi.03_gold.daily_trips_summary")
df2.display()

# COMMAND ----------

