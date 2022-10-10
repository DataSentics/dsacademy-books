# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books_ratings = spark.readStream.table("books_ratings_bronze")

df_books_ratings_cleansed = df_books_ratings.withColumn(
    "Book-Rating",
    f.when(f.col("Book-Rating") == 0, None).otherwise(f.col("Book-Rating")),
).withColumn("User-ID", f.col("User-ID").cast("bigint")).drop(f.col("_rescued_data"))

# COMMAND ----------

books_ratings_path_upload_2 = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/books_ratings"
)

# COMMAND ----------

df_books_ratings_cleansed.createOrReplaceTempView("books_ratings_silver_tempView")

# COMMAND ----------

spark.table("books_ratings_silver_tempView").writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_ratings_silver_checkpoint/",
).option("path", books_ratings_path_upload_2).outputMode(
    "append"
).table(
    "books_ratings_silver"
)
