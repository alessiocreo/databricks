# Databricks notebook source
# MAGIC %md
# MAGIC pitstops

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

pitstop_schema = StructType(fields=[
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstop_df = spark.read.schema(pitstop_schema).option("multiLine",True).json("/mnt/datameshposte/raw/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstop_df = pitstop_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

pitstop_df.write.parquet("/mnt/datameshposte/processed/pit_stops")
