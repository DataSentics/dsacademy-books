# Databricks notebook source
# run autoloader using data_source, source_format, checkpoint_directory, delimiter
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

# MAGIC %run ../WriteFunction

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

Loading_userspii = auto_loader(
    users_pii_path,
    "json",
    checkpoint_users_pii_path,
    ",",
)

# COMMAND ----------

WriteFunction(
    Loading_userspii,
    checkpoint_write_users_pii_path,
    users_pii_output_path,
    "bronze_pii"
)
