# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# cleaning and reading the data from bronze books
books_df = (
    spark.readStream.table("bronze_books")
    .fillna("unknown")
    .withColumn(
        "Year-Of-Publication",
        when(col("Year-Of-Publication") == "0", "unknown").otherwise(
            col("Year-Of-Publication")
        ),
    )
)

# COMMAND ----------

(
    books_df
    .writeStream
    .format("delta").option("checkpointLocation", checkpoint_books_path)
    .option("path", books_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_books")
)
