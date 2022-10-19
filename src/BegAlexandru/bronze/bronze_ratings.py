# Databricks notebook source
# run autoloader using data_source, source_format, checkpoint_directory, delimiter
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

loading_ratings = auto_loader(
    ratings_path,
    "csv",
    checkpoint_ratings_path,
    ";",
)

# COMMAND ----------

write_stream_azure_append(
    loading_ratings,
    checkpoint_write_ratings_path,
    books_rating_output_path,
    "bronze_ratings"
)
