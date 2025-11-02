# Databricks notebook source
import urllib.request
import json
import os
from datetime import datetime
from datetime import *
from dateutil.relativedelta import relativedelta    
import shutil

# COMMAND ----------

two_month_ago=(datetime.now() - relativedelta(months=3)).strftime('%Y-%m')
dir_path=f"/Volumes/nyctaxi/00_landing/data_source/nyctaxi_yellow/{two_month_ago}"
local_path=f"{dir_path}/yellow_tripdata_{two_month_ago}.parquet"

try:
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value='yes')
    print("File already downloaded, aborting downstream")
except:
    try:
        url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{two_month_ago}.parquet"
        response=urllib.request.urlopen(url)
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)
        dbutils.jobs.taskValues.set(key="continue_downstream", value='yes')
        print("Downloaded file")
    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value='no')
        print(f"File not found {str(e)}")