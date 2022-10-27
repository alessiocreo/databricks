# Databricks notebook source
# MAGIC %md
# MAGIC Json File

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("/mnt/datameshposte/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC delete url column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructor_df = constructor_df.drop(col('url'))
display(constructor_df)

# COMMAND ----------

constructor_df_final = constructor_df.withColumnRenamed("constructorId","constructor_id")\
                                     .withColumnRenamed("constructorRef","constructor_ref")\
                                     .withColumn("ingestion_date",current_timestamp())

display(constructor_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC write to parquet

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datameshposte/processed

# COMMAND ----------

constructor_df_final.write.mode('overwrite').parquet('/mnt/datameshposte/processed/constructors')
