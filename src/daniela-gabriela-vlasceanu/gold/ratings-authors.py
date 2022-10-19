# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_joined = spark.table("books_joined_silver")

books_joined_df = books_joined.groupBy("Book-Author").agg(
    f.count("User-ID").alias("Number_of_ratings"),
    f.avg("Book-Rating").alias("Rating-Average"),
)

# COMMAND ----------

books_joined_df.createOrReplaceTempView("ratings_authors_TempView")
spark.sql(
    "CREATE OR REPLACE TABLE ratings_authors AS SELECT * FROM ratings_authors_TempView"
)
