# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS radomirfabian_books

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE radomirfabian_books

# COMMAND ----------

ratings_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("radomirfabian")
    + "BX-Book-Ratings.csv"
)

# COMMAND ----------

df_ratings = (
    spark.read
    .option("encoding", "UTF-8")
    .option("charset", "iso-8859-1")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(ratings_path)
)


# COMMAND ----------

display(df_ratings)

# COMMAND ----------

df_ratings.write.mode('overwrite').saveAsTable("bronze_ratings")

# COMMAND ----------

ratings_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/ratings'
)

# COMMAND ----------

df_ratings.write.parquet(ratings_output_path, mode='overwrite')

# COMMAND ----------


