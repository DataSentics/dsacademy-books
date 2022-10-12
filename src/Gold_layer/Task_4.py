# Databricks notebook source
# MAGIC %md
# MAGIC #### The user's country who rated the most books published after the year 2000

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

df_books_rating = (spark.table("silver_ratings"))

df_books = (spark.table("silver_books"))

df_users = spark.table("silver_users")

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
