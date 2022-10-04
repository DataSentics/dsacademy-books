# Databricks notebook source
# MAGIC %md
# MAGIC #### 10 best-rated authors in total

# COMMAND ----------

from pyspark.sql.functions import col, mean, count

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books"
)

# COMMAND ----------

df_books_rating = (spark.read.parquet(books_rating_path))

df_books = (spark.read.parquet(books_path))

# COMMAND ----------

new_df = df_books.select(
    ["Book_Title", "Publisher", "Book_Author", "Year_Of_Publication", "ISBN"]
)

new_df2 = df_books_rating.select("Book_Rating", "ISBN")

new_df = new_df.join(new_df2, "ISBN", how="inner")

new_df = new_df.groupBy(["Book_Author", "Book_Rating"]).agg(
    mean("Book_Rating").alias("Rating_score"),
    count("Book_Rating").alias("Nr_of_ratings"),
)

new_df = (
    new_df.withColumn("Rating_score", col("Rating_score"))
    .sort(col("Rating_score").desc(), col("Nr_of_ratings").desc())
    .limit(10)
)
