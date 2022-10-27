# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

#{"resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,"time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1}

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), False),
    StructField("positionText", IntegerType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", IntegerType(), False),
    StructField("milliseconds", IntegerType(), False),
    StructField("fastestLap", IntegerType(), False),
    StructField("rank", IntegerType(), False),
    StructField("fastestLapTime", StringType(), False),
    StructField("fastestLapSpeed", DoubleType(), False),
    StructField("statusId", IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/datameshposte/raw/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
results_df = results_df.withColumnRenamed("resultId","result_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("fastestLap","fastest_lap") \
    .withColumnRenamed("fastestLapTime","fastest_lap_time")  \
    .withColumnRenamed("positionText","position_text") \
    .withColumnRenamed("positionOrder","position_order")  \
    .withColumn("ingestion_date", current_timestamp()) \
    .drop("statusId")


# COMMAND ----------

results_df.write.partitionBy('race_id').parquet("/mnt/datameshposte/processed/results")
