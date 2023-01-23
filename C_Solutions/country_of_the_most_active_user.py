# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

from pyspark.sql.functions import col

df_users_cleaned = spark.table('users_silver')
df_books_rating_cleaned = spark.table('book_ratings_silver')
df_books_cleaned = spark.table('books_silver')
df_merged = (df_users_cleaned.join(df_books_rating_cleaned, "User-ID")
                            .join(df_books_cleaned, 'ISBN')
                            .where(col('Year-Of-Publication') > 1999))
df_merged = df_merged.groupBy('User-ID', 'country').count()
answer = df_merged.sort('count', ascending = [False])

(answer.write
.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.option("path", f'{answer_question}/user_origin_country')
.saveAsTable("user_origin_country_answer"))




# COMMAND ----------

display(spark.table("user_origin_country_answer"))
