# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# run Write Function using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# cleaning and reading the data from bronze books
books_df = (
    spark.readStream.table("bronze_books")
    .withColumn(
        "Year-Of-Publication",
        when(col("Year-Of-Publication") == "0", None).otherwise(
            col("Year-Of-Publication")
        ),
    )
)

# COMMAND ----------

write_stream_azure_append(
    books_df,
    checkpoint_books_path,
    books_output_path,
    "silver_books"
)
