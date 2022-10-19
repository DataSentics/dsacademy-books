# Databricks notebook source
# run autoloader using data_source, source_format, checkpoint_directory, delimiter
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

loading_data = auto_loader(
    books_path,
    "csv",
    checkpoint_books_path,
    ";",
)

# COMMAND ----------

write_stream_azure_append(
    loading_data,
    checkpoint_write_books_path,
    books_output_path,
    "bronze_books"
)
