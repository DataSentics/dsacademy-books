# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS radomirfabian_books

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE radomirfabian_books

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("radomirfabian")
    + "BX-Books.csv"
)

# COMMAND ----------

df_books = (
    spark.read
    .option("encoding", "UTF-8")
    .option("charset", "iso-8859-1")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(books_path)
)


# COMMAND ----------

display(df_books)

# COMMAND ----------

df_books.write.mode('overwrite').saveAsTable("bronze_books")

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/books'
)

# COMMAND ----------

df_books.write.parquet(books_output_path, mode='overwrite')

# COMMAND ----------


