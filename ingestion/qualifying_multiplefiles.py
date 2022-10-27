# Databricks notebook source
# MAGIC %md
# MAGIC laptimes

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

laptimes_schema = StructType(fields=[
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read.schema(laptimes_schema).csv("/mnt/datameshposte/raw/lap_times/lap_times*csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
laptimes_df = laptimes_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

laptimes_df.write.mode('overwrite').parquet("/mnt/datameshposte/processed/lap_times")
