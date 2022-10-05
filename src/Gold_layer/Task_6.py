# Databricks notebook source
# MAGIC %md
# MAGIC ####Average book rating per gender (men/women) and per age group (groups: 0-10, 11-20)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, floor

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/users_3nf"
)

ratings_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

# COMMAND ----------

df_users = spark.read.parquet(users_path)

df_ratings = spark.read.parquet(ratings_path)

# COMMAND ----------

df_users = df_users.join(df_ratings, "User_ID")

# COMMAND ----------

df_users = (
    df_users.withColumn("Age_categ", floor(col("Age") - (col("Age") % 10)))
    .withColumn(
        "Age_categ", concat((col("Age_categ") + 1), lit(" - "), (col("Age_categ") + 10))
    )
    .groupBy("Age_categ", "gender")
    .agg(mean("Book_Rating").alias("Avg_book_ratings"))
    .sort(col("Age_categ"))
)
