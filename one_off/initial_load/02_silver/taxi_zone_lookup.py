# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.window import Window
from DeltaTable import *
from datetime import datetime

# COMMAND ----------

df=spark.read.format('csv').option('header', True).load('/Volumes/nyctaxi/00_landing/data_source/lookup/taxi_zone_lookup.csv')

# COMMAND ----------

df=df.select(
    F.col('LocationID').cast('int').alias('location_id'),
    F.col('Borough').alias('borough'),
    F.col('Zone').alias('zone'),
    F.col('service_zone'),
    F.current_timestamp().alias('effective_date'),
    F.lit(None).cast(TimestampType()).alias('end_date')
)

# COMMAND ----------

end_timestamp=datetime.now()
dt=DeltaTable.forName(spark, 'nyctaxi.02_silver.taxi_zone_lookup')
dt.alias('t').merge(
    source=df.alias('s'),
    condition="t.location_id=s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)")\
    .whenMatchedUpdate(
        set={"t.end_date":F.lit(end_timestamp).cast(TimestampType())}
    )\
    .exceute()

# COMMAND ----------

insert_id_list=[row.location_id for row in dt.toDF().filter(f"end_date='{end_timestamp}'").select('location_id').collect()]
if len(insert_id_list)==0:
    print('No new records to insert')
else:
    dt.alias("t")\
    .merge(
        source=df.alias('s'),
        condition=f"s.location_id not in ({', '}.join(map(str, insert_id_list)))"
    )\
    .whenNotMatchedInsert(
        values={
            "t.location_id":"s.location_id",
            "t.borough":"s.borough",
            "t.zone":"s.zone",
            "t.service_zone":"s.service_zone",
            "t.effective_date":current_timestamp(),
            "t.end_date":lit(None).cast(TimestampType())
        }
    )\
    .execute()

# COMMAND ----------

dt.alias('t')\
    .merge(
        source=df.alias('s'),
        condition="t.location_id = s.location_id AND"
    )\
    .whenNotMatchedInsert(
        values={
            "t.location_id":"s.location_id",
            "t.borough":"s.borough",
            "t.zone":"s.zone",
            "t.service_zone":"s.service_zone",
            "t.effective_date":current_timestamp(),
            "t.end_date":lit(None).cast(TimestampType())
        }
    )\
    .execute()