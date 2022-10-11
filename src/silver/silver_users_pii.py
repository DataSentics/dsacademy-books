# Databricks notebook source
import time

# COMMAND ----------

# MAGIC %run ../includes/includes_silver

# COMMAND ----------

#Read users pii
df_pii = spark.readStream.table("bronze_pii")

# COMMAND ----------

dbutils.fs.rm(checkpoint_pii_path, True)

# COMMAND ----------

(
    df_pii
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_pii_path)
    .option("path", pii_output_path)
    .trigger(once = True)
    .outputMode("append")
    .table("silver_pii")
)

# COMMAND ----------

time.sleep(10)
