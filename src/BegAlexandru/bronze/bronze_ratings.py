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

Loading_ratings = auto_loader(
    ratings_path,
    "csv",
    checkpoint_ratings_path,
    ";",
)

# COMMAND ----------

WriteFunction(
    Loading_ratings,
    checkpoint_write_ratings_path,
    books_rating_output_path,
    "bronze_ratings"
)
