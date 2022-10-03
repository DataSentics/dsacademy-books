# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('andreitugmeanu') + 'BX-Books.csv'

# COMMAND ----------

df_books = (spark
            .read
            .format("csv")
            .option("encoding", "ISO-8859-1")
            .option("header", "true")
            .option("sep", ";")
            .load(books_path)
           )

# COMMAND ----------

df_books.write.mode('overwrite').saveAsTable("bronze_book")

# COMMAND ----------

books_output_path = ('abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AT_books/Bronze/books')

# COMMAND ----------

df_books.write.parquet(books_output_path, mode='overwrite')

# COMMAND ----------


