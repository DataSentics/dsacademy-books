# Databricks notebook source
# MAGIC %md
# MAGIC #### 10 best-rated authors by year of publication and publishers

# COMMAND ----------

from pyspark.sql.functions import col, avg, count

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

df_books_rating = (spark.table("silver_ratings"))

df_books = (spark.table("silver_books"))

# COMMAND ----------

df_books = df_books.select(
    ["Book_Title", "Publisher", "Book_Author", "Year_Of_Publication", "ISBN"]
)

df_books_rating = df_books_rating.select("Book_Rating", "ISBN")

aux_df = df_books.join(df_books_rating, "ISBN", how="inner")

aux_df = aux_df.groupBy(["Publisher", "Year_Of_Publication"]).agg(
    avg("Book_Rating").alias("Rating_score"),
    count("Book_Rating").alias("Nr_of_ratings"),
)

result_df = aux_df.sort(col("Rating_score").desc(), col("Nr_of_ratings").desc()).limit(10)

display(result_df)