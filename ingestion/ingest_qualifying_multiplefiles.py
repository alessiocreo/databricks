# Databricks notebook source
# MAGIC %md
# MAGIC qualifying

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
#constructorId
qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine',True).json("/mnt/datameshposte/raw/qualifying/*json")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualifying_df = qualifying_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("cons","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

qualifying_df.write.mode('overwrite').parquet("/mnt/datameshposte/processed/qualifying")
