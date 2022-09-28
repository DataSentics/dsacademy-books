# Databricks notebook source
#paths for raw data
book_ratings_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('begalexandrunarcis') + 'BX-Book-Ratings.csv'
books_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('begalexandrunarcis') + 'BX-Books.csv'
users_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('begalexandrunarcis') + 'BX-Users.csv'

# COMMAND ----------

df_books_rating = spark.read.option("header", "true").option("delimiter",";").csv(book_ratings_path)
book_shema = df_books_rating.schema

# COMMAND ----------


stream = (
  spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("inferSchema","true")
 .schema(book_shema)
 .load(book_ratings_path)
 .writeStream
 .option("checkpointLocation", "/local_disk0/tmp/")
 .option("mergeSchema", "true")
 .outputMode("append")  
 .table("book_ratings_bronze")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM book_ratings_bronze

# COMMAND ----------


