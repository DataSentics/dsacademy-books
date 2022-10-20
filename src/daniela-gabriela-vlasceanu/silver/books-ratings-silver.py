# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books_ratings = spark.readStream.table("books_ratings_bronze")

df_books_ratings_cleansed = (
    df_books_ratings
    .withColumn(
        "Book_Rating",
        f.when(f.col("Book-Rating") == 0, None)
        .otherwise(f.col("Book-Rating")))
    .withColumn("User_ID", f.col("User-ID").cast("bigint"))
    .select("ISBN", "Book_Rating", "User_ID")
)

# COMMAND ----------

books_ratings_path_upload_2 = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/books_ratings"
)

# COMMAND ----------

df_books_ratings_cleansed.createOrReplaceTempView("books_ratings_silver_tempView")

# COMMAND ----------

(
    spark.table("books_ratings_silver_tempView")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_books_ratings_silver_checkpoint/",
    )
    .option("path", books_ratings_path_upload_2)
    .outputMode("append")
    .table("books_ratings_silver")
)
