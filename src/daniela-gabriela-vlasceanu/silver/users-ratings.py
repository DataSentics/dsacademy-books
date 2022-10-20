# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver").drop("_rescued_data")
users_df = spark.table("users_joined_pii_silver").drop("_rescued_data")

df = books_df.join(users_df, "User_ID")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("users_ratings")
