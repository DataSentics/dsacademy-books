# Databricks notebook source
# MAGIC %md
# MAGIC #### The user's country who rated the most books published after the year 2000

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books"
)

users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users"
)

# COMMAND ----------

df_books_rating = spark.read.parquet(books_rating_path)

df_books = spark.read.parquet(books_path)

df_users = spark.read.parquet(users_path)

# COMMAND ----------

df_books_rating = df_books_rating.select("Book_Rating", "ISBN", "User_ID")

df_books = df_books.select(["Year_Of_Publication", "ISBN"])
 
df_users = df_users.select(["Country", "User_ID"])

aux_df = df_books_rating.join(df_books, "ISBN", how="inner")

aux_df = aux_df.join(df_users, "User_ID", how="inner")

aux_df = aux_df.where(col("Book_Rating").isNotNull()).filter(
    "Year_Of_Publication > 2000"
)

aux_df = aux_df.groupBy(["User_ID", "Country"]).count()

result_df = (
    aux_df.withColumnRenamed("count", "Number_of_ratings")
    .sort(col("Number_of_ratings").desc())
    .filter("Country != 'n/a'")
    .limit(1)
)
