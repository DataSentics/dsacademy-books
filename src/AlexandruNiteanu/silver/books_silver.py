# Databricks notebook source
# notebook for cleaning the data
# used for books file

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_books = (
    spark.readStream.table("bronze_books")
    .withColumn(
        "Year-Of-Publication",
        when(col("Year-Of-Publication") == 0, None).otherwise(
            col("Year-Of-Publication")
        ),
    )
)

# COMMAND ----------

# the col Year-Of-Publication was full of 0 so I replaced them with null
df_books.writeStream.format("delta").option(
    "checkpointLocation",
    books_checkpoint,
).option("path", books_path_cleansed).trigger(availableNow=True).outputMode("append").table("books_silver")
