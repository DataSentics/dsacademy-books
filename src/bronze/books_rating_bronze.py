# Databricks notebook source
#notebook for reading the data from the storage and write it as parquet
#used for BX-Books-Rating

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the csv
path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("alexandruniteanu")
    + "BX-Book-Ratings.csv"
)
# path for writing the csv as parquet
path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books_rating"
)

# COMMAND ----------

# saving the csv into a df
df = spark.read.option("header", "true").option("delimiter", ";").csv(path_reading)

# COMMAND ----------

#registering the table in the metastore
df.write.mode("overwrite").saveAsTable("books_rating_raw")

# COMMAND ----------

#writing it as parquet in the azure storage
df.write.parquet(path_writing, mode='overwrite')
