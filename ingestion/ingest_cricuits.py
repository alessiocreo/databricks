# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datameshposte/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), False),
    StructField("name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False),
    StructField("alt", IntegerType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

circuits_df = spark.read.schema(circuits_schema).csv("dbfs:/mnt/datameshposte/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

circuits_df.show(truncate=False)
circuits_df.printSchema()
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","country","lat","lng","alt")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the columns as required

# COMMAND ----------

circuits_renames = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") 
    

# COMMAND ----------

display(circuits_renames)

# COMMAND ----------

# MAGIC %md
# MAGIC #Add ingestion time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuits_final_df = circuits_renames.withColumn("ingestion_date",current_timestamp()).withColumn("env",lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write to data lake

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datameshposte/processed

# COMMAND ----------



# COMMAND ----------

circuits_final_df.write.parquet("/mnt/datameshposte/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datameshposte/processed/circuits
