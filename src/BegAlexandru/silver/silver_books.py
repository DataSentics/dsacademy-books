# Databricks notebook source
from pyspark.sql.functions import col, when

# COMMAND ----------

# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../WriteFunction

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

WriteFunction(books_df, 
              checkpoint_books_path, 
              books_output_path, 
              "silver_books",
             )
