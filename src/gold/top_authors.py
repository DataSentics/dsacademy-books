# Databricks notebook source
# 10 best-rated authors in total

# COMMAND ----------

from pyspark.sql.functions import col, avg

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

df = spark.sql("select * from ANiteanuBooks.users_rating_books")

# COMMAND ----------

df = (
    df.groupBy("Book-Author")
    .agg(avg("Book-Rating"))
    .orderBy(col("avg(Book-Rating)").desc())
    .limit(10)
)
