# Databricks notebook source
# notebook for cleaning the data
# used for books_rating file

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the data
reading_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books_rating"
)
# path for writing back to the storage the cleaned data
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/books_rating_silver"
)

# COMMAND ----------

# saving the data into a dataframe
df=spark.read.parquet(reading_path)

# COMMAND ----------

# the col Book-Rating was full of 0 so I replaced them with null
df = df.withColumn(
    "Book-Rating",
    when(col("Book-Rating") == 0, None).otherwise(col("Book-Rating")),
)

# COMMAND ----------

# registering the table in the metastore
df.write.mode("overwrite").saveAsTable("books_rating_silver")

# COMMAND ----------

# writing it to the storage
df.write.parquet(writing_path, mode='overwrite')
