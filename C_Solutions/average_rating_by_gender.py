# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import avg

df_users = spark.table("users_pii_silver")
df_ratings = spark.table("book_ratings_silver")
df_merged = df_users.join(df_ratings, "User-ID")
df_merged = df_merged.groupBy('gender').agg(avg('Book-Rating').alias('Average-Book_Rating'))

display(df_merged)

# COMMAND ----------

(df_merged.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", f'{answer_question}/ratings_by_gender')
 .saveAsTable("ratings_by_gender_answer"))
