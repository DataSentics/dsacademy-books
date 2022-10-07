# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_authors_ratings = spark.table("books_ratings_silver")
df_books = spark.table("books_silver")

df_joined = df_authors_ratings.join(df_books, "ISBN")
# display(df_joined)

# COMMAND ----------

authors_pub_years = (
    df_joined.groupBy("Book-Author", "Year-Of-Publication", "Publisher")
    .agg(
        f.count("User-ID").alias("How_many_ratings"),
        f.avg("Book-Rating").alias("Rating-Average"),
    )
    .dropna()
)
# display(authors_pub_years)

# COMMAND ----------

# %sql
# Select How_many_ratings, `Year-Of-Publication`, `Book-Author` from authors_pub_years
# where Publisher == "Pearson Higher Education" and `Year-Of-Publication` == 2001


# COMMAND ----------

authors_pub_years.createOrReplaceTempView("authors_pub_years_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE authors_pub_years AS SELECT * FROM authors_pub_years_tempView"
)
