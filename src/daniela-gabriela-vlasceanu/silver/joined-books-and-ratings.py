# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

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

upload_path = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/books_joined_silver"
)

# COMMAND ----------

(
    books_joined
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("books_joined_silver")
)
