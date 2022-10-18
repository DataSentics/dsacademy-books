# Databricks notebook source
# 10 best-rated authors by year of publication and publishers

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, avg

# COMMAND ----------

df_users_rating_books = spark.table("users_rating_books")

# COMMAND ----------

df_result = (
    df_users_rating_books.groupBy("Book-Author", "Publisher", "Year-Of-Publication")
    .agg(avg("Book-Rating").alias("Average-Rating"))
    .orderBy(col("Average-Rating").desc())
    .limit(10)
)

# COMMAND ----------

df_result.createOrReplaceTempView("top_authors_by_year_publisher")
