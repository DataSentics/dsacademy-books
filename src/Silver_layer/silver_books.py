# Databricks notebook source
from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books"
)

# COMMAND ----------

df_books = (spark.read.parquet(books_path))

# COMMAND ----------

df_books = (
    spark.read.parquet(books_path)
    .withColumnRenamed("Book-Title", "Book_Title")
    .withColumnRenamed("Book-Author", "Book_Author")
    .withColumnRenamed("Year-Of-Publication", "Year_Of_Publication")
    .withColumnRenamed("Image-URL-S", "Image_RL_S")
    .withColumnRenamed("Image-URL-M", "Image_URL_M")
    .withColumnRenamed("Image-URL-L", "Image_URL_L")
    .withColumn(
        "Year_Of_Publication",
        when(col("Year_Of_Publication") == "0", "Unknown").otherwise(
            col("Year_Of_Publication")
        ),
    )
    .fillna("unknown")
)

# COMMAND ----------

df_books.write.mode('overwrite').saveAsTable("silver_books")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books"
)

# COMMAND ----------

df_books.write.parquet(output_path, mode='overwrite')
