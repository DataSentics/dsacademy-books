# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

book_rating_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Books-Ratings/".format(
    "danielavlasceanu-gdc-final-task"
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_ratings_raw_checkpoint/",
    )
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(book_rating_path)
    .createOrReplaceTempView("books_ratings_raw_tempView")
)

# COMMAND ----------

spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW books_ratings_bronze_temp AS SELECT * FROM books_ratings_raw_tempView"
)

# COMMAND ----------

books_ratings_path_upload = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/books-ratings"
)

# COMMAND ----------

spark.table("books_ratings_bronze_temp").writeStream.trigger(availableNow=True).format("delta").option(
    "checkpointLocation",
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_ratings_raw_checkpoint/",
).option("path", books_ratings_path_upload).outputMode("append").table("books_ratings_bronze")
