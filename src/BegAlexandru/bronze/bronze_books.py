# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "BX-Books.csv"
)

# COMMAND ----------

df_books = (
    spark.read.option("encoding", "IEC-8859-1")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(books_path)
)


# COMMAND ----------

df_books.write.mode('overwrite').saveAsTable("bronze_books")

# COMMAND ----------

display(df_books)

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'AlexB_Books/bronze/books'
)

# COMMAND ----------

df_books.write.parquet(books_output_path, mode='overwrite')
