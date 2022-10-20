# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books = spark.readStream.table("books_bronze")

df_books_cleansed = (
    df_books
    .withColumn("Book_Title", f.initcap(f.col("Book-Title")))
    .withColumn(
        "Year_Of_Publication",
        f.when(f.col("Year-Of-Publication") == 0, None)
        .otherwise(f.col("Year-Of-Publication"))
    )
    .withColumnRenamed("Book-Author", "Book_Author")
    .withColumnRenamed("Image-URL-S", "Image_URL_S")
    .withColumnRenamed("Image-URL-M", "Image_URL_M")
    .withColumnRenamed("Image-URL-L", "Image_URL_L")
    .drop(f.col("Book-Title"))
    .drop(f.col("Year-Of-Publication"))
    .drop(f.col("_rescued_data"))
)

# COMMAND ----------

books_path_upload_2 = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/books"
)

# COMMAND ----------

df_books_cleansed.createOrReplaceTempView("books_silver_tempView")

# COMMAND ----------

(
    spark.table("books_silver_tempView")
    .writeStream
    .trigger(availableNow=True)
    .format("delta").option(
        "checkpointLocation",
        f"{working_dir}daniela_books_silver_checkpoint/",
    )
    .option("path", books_path_upload_2)
    .outputMode("append")
    .table("books_silver")
)
