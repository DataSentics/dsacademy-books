# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books_ratings = spark.table("books_ratings_bronze")

df_books_ratings_cleansed = (df_books_ratings
                             .withColumn("Book-Rating",f.when(f.col("Book-Rating") == 0, None).otherwise(f.col("Book-Rating")))
                             .withColumn("User-ID", f.col("User-ID").cast('bigint'))
                            )


# COMMAND ----------

books_ratings_path_upload_2 = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata') + 'daniela-vlasceanu-books/silver/books_ratings'

df_books_ratings_cleansed.write.parquet(books_ratings_path_upload_2, mode='overwrite')

# COMMAND ----------

df_books_ratings_cleansed.createOrReplaceTempView("books_ratings_silver_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE books_ratings_silver AS SELECT * FROM books_ratings_silver_tempView"
)
