# Databricks notebook source
from pyspark.sql.functions import when, col
import time

# COMMAND ----------

# MAGIC %run ../includes/includes_silver

# COMMAND ----------

# Ingest & clean data from bronze books
books_df = (
    spark.readStream
    .table("bronze_books")
    .fillna("unknown")
    .withColumn("Year-Of-Publication",
                when(col("Year-Of-Publication") == "0", "unknown")
                .otherwise(col("Year-Of-Publication"))
               )
)

# COMMAND ----------

(
    books_df
    .writeStream
    .format("delta").option("checkpointLocation", checkpoint_books_path)
    .option("path", books_output_path)
    .outputMode("append")
    .table("silver_books")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_books_path, True)

# COMMAND ----------


