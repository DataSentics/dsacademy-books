# Databricks notebook source
# run autoloader using data_source, source_format, checkpoint_directory, delimiter
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

loading_users = auto_loader(
    users_path,
    "csv",
    checkpoint_users_path,
    ";",
)

# COMMAND ----------

write_stream_azure_append(
    loading_users,
    checkpoint_write_users_path,
    users_output_path,
    "bronze_users"
)
