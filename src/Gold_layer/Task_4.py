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

new_df1 = df_books_rating.select("Book_Rating", "ISBN", "User_ID")

new_df2 = df_books.select(["Year_Of_Publication", "ISBN"])

new_df3 = df_users.select(["Country", "User_ID"])

new_df = new_df1.join(new_df2, "ISBN", how="inner")

new_df = new_df.join(new_df3, "User_ID", how="inner")

new_df = new_df.where(col("Book_Rating").isNotNull()).filter(
    "Year_Of_Publication > 2000"
)

new_df = new_df.groupBy(["User_ID", "Country"]).count()

new_df = (
    new_df.withColumnRenamed("count", "Number_of_ratings")
    .sort(col("Number_of_ratings").desc())
    .filter("Country != 'n/a'")
    .limit(1)
)
