# Databricks notebook source
# MAGIC %run ../includes/includes_bronze

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

Loading_ratings = auto_loader(
    ratings_path,
    "csv",
    checkpoint_ratings_path,
    ";",
)

# COMMAND ----------

(
    Loading_ratings
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_ratings_path)
    .option("path", books_rating_output_path)
    .option("overwriteSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_ratings")
)
