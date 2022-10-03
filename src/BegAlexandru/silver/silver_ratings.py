# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC Use alexandru_beg_books

# COMMAND ----------

#cleaning the data from bronze rating

# COMMAND ----------

books_rating_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') 
    + 'BegAlex_Books/bronze/books_rating'
)

# COMMAND ----------

df_rating = spark.read.parquet(books_rating_path).withColumn(
    "Book-Rating", col("Book-Rating").cast("Integer")
)

# COMMAND ----------

df_rating.write.mode('overwrite').saveAsTable("silver_rating")

# COMMAND ----------

rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/''.format("03cleanseddata")
    + "AlexB_Books/silver/ratings"
)

# COMMAND ----------

df_rating.write.parquet(rating_output_path, mode='overwrite')
