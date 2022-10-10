# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_1 = spark.table("books_silver")
df_2 = spark.table("books_ratings_silver")

df_books = df_1.drop(f.col("_rescued_data"))
df_books_ratings = df_2.drop(f.col("_rescued_data"))

# COMMAND ----------

books_joined = df_books.join(df_books_ratings, "ISBN")

# COMMAND ----------

books_joined.createOrReplaceTempView("books_joined_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE books_joined_silver AS SELECT * FROM books_joined_tempView"
)
