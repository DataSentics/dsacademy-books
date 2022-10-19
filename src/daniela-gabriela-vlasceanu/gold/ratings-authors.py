# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_joined = spark.table("books_joined_silver")

books_joined_df = books_joined.groupBy("Book-Author").agg(
    f.count("User-ID").alias("Number-of-ratings"),
    f.avg("Book-Rating").alias("Rating-Average"),
)

# COMMAND ----------

books_joined_df.write.mode("overwrite").saveAsTable("ratings_authors")
