# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "BX-Book-Ratings.csv"
)

# COMMAND ----------

df_rating = (
    spark.read.option("header", "true").option("delimiter", ";").csv(rating_path)
)

# COMMAND ----------

df_rating.write.mode('overwrite').saveAsTable("bronze_ratings")

# COMMAND ----------

books_rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_rating'
)

# COMMAND ----------

df_rating.write.parquet(books_rating_output_path, mode='overwrite')
