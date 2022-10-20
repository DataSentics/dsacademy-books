# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_authors_ratings = spark.table("books_ratings_silver").drop("_rescued_data")
df_books = spark.table("books_silver").drop("_rescued_data")

df_joined = df_authors_ratings.join(df_books, "ISBN")

# COMMAND ----------

authors_pub_years = (
    df_joined.groupBy("Book_Author", "Year_Of_Publication", "Publisher")
    .agg(
        f.count("User_ID").alias("Number_of_ratings"),
        f.avg("Book_Rating").alias("Rating_Average"),
    )
    .dropna()
)
# display(authors_pub_years)

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("04golddata")
    + "daniela-vlasceanu-books/gold/authors_pub_years"
)

# COMMAND ----------

(
    authors_pub_years
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("authors_pub_years")
)
