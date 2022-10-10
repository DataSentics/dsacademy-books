# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Books/".format(
    "danielavlasceanu-gdc-final-task"
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_raw_checkpoint/",
    )
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(books_path)
    .createOrReplaceTempView("books_raw_temp")
)

# COMMAND ----------

books_path_upload = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/books"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace Temporary view books_bronze_tmp as select * from books_raw_temp

# COMMAND ----------

spark.table("books_bronze_tmp").writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_raw_checkpoint/",
).option("path", books_path_upload).outputMode("append").table("books_bronze")
