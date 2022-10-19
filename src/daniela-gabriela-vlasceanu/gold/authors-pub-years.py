# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_authors_ratings = spark.table("books_ratings_silver").drop("_rescued_data")
df_books = spark.table("books_silver").drop("_rescued_data")

df_joined = df_authors_ratings.join(df_books, "ISBN")

# COMMAND ----------

authors_pub_years = (
    df_joined.groupBy("Book-Author", "Year-Of-Publication", "Publisher")
    .agg(
        f.count("User-ID").alias("Number_of_ratings"),
        f.avg("Book-Rating").alias("Rating-Average"),
    )
    .dropna()
)
# display(authors_pub_years)

# COMMAND ----------

authors_pub_years.createOrReplaceTempView("authors_pub_years_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE authors_pub_years AS SELECT * FROM authors_pub_years_tempView"
)
