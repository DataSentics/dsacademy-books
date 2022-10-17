# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver").drop("_rescued_data")
users_df = spark.table("users_joined_pii_silver").drop("_rescued_data")

df = books_df.join(users_df, "User-ID")

# COMMAND ----------

df.createOrReplaceTempView("users_ratings_TempView")
spark.sql(
    "CREATE OR REPLACE TABLE users_ratings AS SELECT * FROM users_ratings_TempView"
)
