# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_path = f"{azure_storage}Books/".format(
    "danielavlasceanu-gdc-final-task"
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        f"{working_dir}daniela_books_raw_checkpoint/",
    )
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(books_path)
    .createOrReplaceTempView("books_raw_temp")
)

# COMMAND ----------

books_path_upload = (
    f"{azure_storage}".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/books"
)

# COMMAND ----------

spark.sql(
    "create or replace Temporary view books_bronze_tmp as select * from books_raw_temp"
)

# COMMAND ----------

(
    spark.table("books_bronze_tmp")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_books_raw_checkpoint/",
    )
    .option("path", books_path_upload)
    .outputMode("append")
    .table("books_bronze")
)
