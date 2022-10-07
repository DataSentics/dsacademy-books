# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# best-rated authors by year of publication and publishers
display(joined_df
        .groupBy("Year-Of-Publication", "Publisher", "Book-Author")
        .agg(avg("Book-Rating").alias("Book-Rating"))
        .orderBy(col("Book-Rating").desc())
        )
