# Databricks notebook source
import pyspark.sql.functions as F
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_months_ago_start=date.today().replace(day=1)-relativedelta(months=3)


# COMMAND ----------

df_trips=spark.read.table('nyctaxi.02_silver.yellow_trips_cleansed').filter(f"processed_timestamp>'{two_months_ago_start}'")
df_zones=spark.read.table('nyctaxi.02_silver.taxi_zone_lookup')

# COMMAND ----------

df_join_1=df_trips.join(df_zones,df_trips['pickup_location_id']==df_zones['location_id'],'left').select(
    df_trips.vendor,
    df_trips.tpep_pickup_datetime,
    df_trips.tpep_dropoff_datetime,
    df_trips.trip_duration,
    df_trips.passenger_count,
    df_trips.trip_distance,
    df_trips.rate,
    df_zones.borough.alias('pickup_borough'),
    df_zones.zone.alias('pickup_zone'),
    df_trips.dropoff_location_id,
    df_trips.payment_type,
    df_trips.fare_amount,
    df_trips.extra,
    df_trips.mta_tax,
    df_trips.tolls_amount,
    df_trips.improvement_surcharge,
    df_trips.total_amount,
    df_trips.congestion_surcharge,
    df_trips.airport_fee,
    df_trips.cbd_congestion_fee,
    df_trips.processed_timestamp
)

# COMMAND ----------

df_join_final=df_join_1.join(df_zones,df_join_1['dropoff_location_id']==df_zones['location_id'],'left').select(
    df_join_1.vendor,
    df_join_1.tpep_pickup_datetime,
    df_join_1.tpep_dropoff_datetime,
    df_join_1.trip_duration,
    df_join_1.passenger_count,
    df_join_1.trip_distance,
    df_join_1.rate,
    df_join_1.pickup_borough,
    df_zones.borough.alias('dropoff_borough'),
    df_zones.zone.alias('dropoff_zone'),
    df_join_1.dropoff_location_id,
    df_join_1.payment_type,
    df_join_1.fare_amount,
    df_join_1.extra,
    df_join_1.mta_tax,
    df_join_1.tolls_amount,
    df_join_1.improvement_surcharge,
    df_join_1.total_amount,
    df_join_1.congestion_surcharge,
    df_join_1.airport_fee,
    df_join_1.cbd_congestion_fee,
    df_join_1.processed_timestamp
)

# COMMAND ----------

df_join_final.write.mode('append').saveAsTable('nyctaxi.02_silver.yellow_trips_enriched')

# COMMAND ----------

