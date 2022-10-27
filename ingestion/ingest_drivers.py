# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(
    fields=[
        StructField("forename",StringType(), True),
        StructField("surname", StringType(), True)
    ]
)

# COMMAND ----------

#{"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

drivers_schema = StructType(
    fields=[
        StructField("driverId",StringType(), False),
        StructField("driverRef",StringType(), True),
        StructField("number",IntegerType(), True),
        StructField("name", name_schema, True),
        StructField("dob",DateType(), True),
        StructField("nationality",StringType(), True),
        StructField("url",StringType(), True)     
    ]
)

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/datameshposte/raw/drivers.json")
display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit
drivers_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("driverRef","driver_ref") \
                            .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname")))\
                            .withColumn("ingestion_date", current_timestamp())\
                            .drop(col("url"))

display(drivers_df)

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet("/mnt/datameshposte/processed/drivers")
