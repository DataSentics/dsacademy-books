# Databricks notebook source
#notebook for reading the data from the storage and write it as parquet
#used for BX-Books

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

#path for reading the csv
books_path_reading = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu') + 'BX-Books.csv'
#path for writing the csv as parquet
books_path_writing = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AN_Books/books'


# COMMAND ----------

#saving the csv into a df
df = spark.read.option("header", "true").option("nullValue", None).option("encoding", "UTF-8").option("encoding","ISO-8859-1").option("delimiter",";").csv(books_path_reading)


# COMMAND ----------

display(df)

# COMMAND ----------

#registering the table in the metastore (tried to make a folder to look like /bronze/books but i couldn't)
df.write.mode("overwrite").saveAsTable("books_raw")


# COMMAND ----------

#writing it as parquet in the azure storage
df.write.parquet(books_path_writing, mode='overwrite')
