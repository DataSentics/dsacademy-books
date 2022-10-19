# Databricks notebook source
# run autoloader using data_source, source_format, checkpoint_directory, delimiter
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

loading_userspii = auto_loader(
    users_pii_path,
    "json",
    checkpoint_users_pii_path,
)

# COMMAND ----------

write_stream_azure_append(
    loading_userspii,
    checkpoint_write_users_pii_path,
    users_pii_output_path,
    "bronze_pii"
)
