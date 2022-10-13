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

Loading_data = auto_loader(
    books_path,
    "csv",
    checkpoint_books_path,
    ";",
)

# COMMAND ----------

WriteFunction(
  Loading_data,
  checkpoint_write_books_path,
  books_output_path,
  "bronze_books"
)
