# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver")

# COMMAND ----------

books_ratings = books_df.groupBy(
    "Book-Title", "Book-Author", "Year-of-Publication"
).agg(f.count("User-ID").alias("How_many_ratings"))

# COMMAND ----------

books_ratings.createOrReplaceTempView("ratings_books_TempView")
spark.sql(
    "CREATE OR REPLACE TABLE ratings_books AS SELECT * FROM ratings_books_TempView"
)
