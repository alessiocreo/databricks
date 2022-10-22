# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("round", IntegerType(), False),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("date", StringType(), False),
    StructField("time", StringType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

races_df = spark.read.schema(races_schema).csv("dbfs:/mnt/datameshposte/raw/races.csv",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------



# COMMAND ----------

races_final_df = races_df.select("raceId","year","round","circuitId","name","date","time").withColumnRenamed(
    "raceId","race_id"
).withColumnRenamed(
    "circuitId","circuit_id"
).withColumnRenamed(
    "year","race_year"
).withColumn(
    "ingestion_date", current_timestamp()
).withColumn(
    "race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')
).drop(
    "date","time"
)

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy("race_year").parquet("/mnt/datameshposte/processed/races")

# COMMAND ----------


