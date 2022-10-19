# Databricks notebook source
# run Write Function using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# the pii json is clean so it does not need anymore cleaning
df_pii = spark.readStream.table("bronze_pii")

# COMMAND ----------

write_stream_azure_append(
    df_pii,
    checkpoint_userspii_path,
    pii_output_path,
    "silver_pii"
)
