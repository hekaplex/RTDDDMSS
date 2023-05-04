# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Generate IoT Data for Root Cause Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA digital_twins

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

import numpy as np
from scipy import signal
import pandas as pd
import plotly.express as px

# COMMAND ----------

rng = np.random.default_rng()

AVERAGE_RPM = 500
HEALTHY_STDDEV_RPM = 0.001*AVERAGE_RPM
FAULTY_STDDEV_RPM = 0.005*AVERAGE_RPM
N_SAMPLES = 1000
N_PERIODS = 30

AVERAGE_TEMPERATURE = 25

sin_input = (np.array(range(N_SAMPLES)) / N_SAMPLES)*N_PERIODS*np.pi

def generate_healthy():
  healthy_sin_component = 0.001*AVERAGE_RPM*np.sin(sin_input + rng.random())
  healthy_fan_speeds = rng.normal(AVERAGE_RPM, HEALTHY_STDDEV_RPM, N_SAMPLES) + healthy_sin_component
  return healthy_fan_speeds

def generate_faulty():
  faulty_square_component = 0.01*AVERAGE_RPM*signal.square(sin_input + rng.random())
  faulty_fan_speeds = rng.normal(AVERAGE_RPM, FAULTY_STDDEV_RPM, N_SAMPLES) + faulty_square_component
  return faulty_fan_speeds

# COMMAND ----------

healthy_df = pd.DataFrame({"DryerFanSpeed": generate_healthy()})
healthy_df["status"] = "OK"
faulty_df = pd.DataFrame({"DryerFanSpeed": generate_faulty()})
faulty_df["status"] = "FAULTY_CONTROLLER"

complete_df = pd.concat([healthy_df, faulty_df], axis=0)
px.scatter(complete_df, y="DryerFanSpeed", color="status", title="Fan Speeds (rpm) for Coating & Drying Station")

# COMMAND ----------

healthy_stations = [
  "CoatingStep-Line1-Munich",
  "CoatingStep-Line1-Shanghai",
  "CoatingStep-Line2-Shanghai",
  "CoatingStep-Line1-Dallas",
  "CoatingStep-Line3-Dallas",
]
faulty_stations = [
  "CoatingStep-Line2-Dallas",  
]
all_stations = healthy_stations + faulty_stations

N_REPEATS = 1
coating_df = pd.DataFrame()

for r in range(N_REPEATS):
  for station in all_stations:
    healthy_df = pd.DataFrame({"dryerFanSpeed": generate_healthy()})
    healthy_df["dryerTemperature"] = (healthy_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
    healthy_df["station_id"] = station
    healthy_df["_index"] = healthy_df.index + healthy_df.index.max()*r
    coating_df = pd.concat([coating_df, healthy_df], axis=0)

for station in healthy_stations:
  healthy_df = pd.DataFrame({"dryerFanSpeed": generate_healthy()})
  healthy_df["dryerTemperature"] = (healthy_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
  healthy_df["station_id"] = station
  healthy_df["_index"] = healthy_df.index + healthy_df.index.max()*(r+1)
  coating_df = pd.concat([coating_df, healthy_df], axis=0)

for station in faulty_stations:
  faulty_df = pd.DataFrame({"dryerFanSpeed": generate_faulty()})
  faulty_df["dryerTemperature"] = (faulty_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
  faulty_df["station_id"] = station
  faulty_df["_index"] = faulty_df.index + healthy_df.index.max()*(r+1)
  coating_df = pd.concat([coating_df, faulty_df], axis=0)
  
schema_name = "digital_twins"
table_name = "battery_coating_analysis"
spark.createDataFrame(coating_df).write.mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from digital_twins.battery_coating_analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### You can now directly query the table you defined in `.saveAsTable()` using Databricks SQL!