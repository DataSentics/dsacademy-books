# Databricks notebook source
import pyspark.sql.functions as f


# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books = spark.table("books_bronze")

df_books_cleansed = df_books.withColumn(
    "Book-Title", f.initcap(f.col("Book-Title"))
).withColumn(
    "Year-Of-Publication",
    f.when(f.col("Year-Of-Publication") == 0, None).otherwise(
        f.col("Year-Of-Publication")
    ),
)
# display(df_books_cleansed)

# COMMAND ----------

books_path_upload_2 = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/books"
)

df_books_cleansed.write.parquet(books_path_upload_2, mode="overwrite")

# COMMAND ----------

df_books_cleansed.createOrReplaceTempView("books_silver_tempView")
spark.sql("CREATE OR REPLACE TABLE books_silver AS SELECT * FROM books_silver_tempView")
