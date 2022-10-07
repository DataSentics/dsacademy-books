# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC Use alexandru_beg_books

# COMMAND ----------

# cleaning the data from bronze rating

# COMMAND ----------

books_rating_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_rating'
)

# COMMAND ----------

df_rating = (
    spark
    .readStream
    .table("bronze_ratings")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
)

# COMMAND ----------

rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'BegAlex_Books/silver/books_rating'
)

# COMMAND ----------

(
    df_rating
    .writeStream
    .format("delta")
    .option("checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_ratings_checkpoint_new/")
    .option("path", rating_output_path)
    .outputMode("append")
    .table("silver_ratings")
)
