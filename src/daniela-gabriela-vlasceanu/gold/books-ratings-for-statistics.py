# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver")

# COMMAND ----------

books_ratings = books_df.groupBy(
    "Book_Title", "Book_Author", "Year_of_Publication"
).agg(f.count("User_ID").alias("Number_of_ratings"))

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("04golddata")
    + "daniela-vlasceanu-books/gold/ratings_books"
)

# COMMAND ----------

(
    books_ratings
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("ratings_books")
)
