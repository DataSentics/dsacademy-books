# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_joined = spark.table("books_joined_silver")

books_joined_df = books_joined.groupBy("Book_Author").agg(
    f.count("User_ID").alias("Number_of_ratings"),
    f.avg("Book_Rating").alias("Rating_Average"),
)

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("04golddata")
    + "daniela-vlasceanu-books/gold/ratings_authors"
)

# COMMAND ----------

(
    books_joined_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("ratings_authors")
)
