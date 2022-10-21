# Databricks notebook source
# From which country is the user who rated the most books published after the year 2000?

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

df_users_rating_books = spark.table("users_rating_books")

# COMMAND ----------

df_result = (
    df_users_rating_books.filter(col("Year-Of-Publication") > "2000")
    .groupBy("User-ID", "country")
    .agg(count("User-ID").alias("Number_of_rated_books"))
    .sort("Number_of_rated_books", ascending=False)
    .limit(1)
)

# COMMAND ----------

df_result.createOrReplaceTempView("user_country_rated_books")
