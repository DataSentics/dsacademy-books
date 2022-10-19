# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver")

# COMMAND ----------

books_ratings = books_df.groupBy(
    "Book-Title", "Book-Author", "Year-of-Publication"
).agg(f.count("User-ID").alias("Number-of-ratings"))

# COMMAND ----------

books_ratings.write.mode("overwrite").saveAsTable("ratings_books")
