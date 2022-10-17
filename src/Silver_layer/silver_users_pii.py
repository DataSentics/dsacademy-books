# Databricks notebook source
# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

df_users_pii = spark.readStream.table("bronze_users_pii").withColumnRenamed(
    "User-ID", "User_ID"
)

# COMMAND ----------

(
    df_users_pii.writeStream.format("delta")
    .option("checkpointLocation", users_pii_checkpoint)
    .option("path", users_pii_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_users_pii")
)
