# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

rating_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('andreitugmeanu') + 'BX-Book-Ratings.csv'

# COMMAND ----------

df_book_ratings = (spark
                   .read
                   .format("csv")
                   .option("header", "true")
                   .option("sep", ";")
                   .load(rating_path)
                  )

# COMMAND ----------

df_book_ratings.write.mode('overwrite').saveAsTable("bronze_book_ratings")

# COMMAND ----------

books_rating_output_path = ('abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AT_books/Bronze/books_ratings')

# COMMAND ----------

df_book_ratings.write.parquet(books_rating_output_path, mode='overwrite')

# COMMAND ----------


