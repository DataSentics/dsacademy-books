# Databricks notebook source
from pyspark.sql.functions import avg

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

silver_books_df = spark.table("silver_books")
silver_ratings_df = spark.table("silver_ratings")

# COMMAND ----------

# renaming the columns _rescued_data from each table and then joining
silver_books_df = (
    silver_books_df
    .withColumnRenamed("_rescued_data", "_rescued_data_books")
)
silver_ratings_df = (
    silver_ratings_df
    .withColumnRenamed("_rescued_data", "_rescued_data_ratings")
)
df_books_with_ratings = silver_books_df.join(silver_ratings_df, on="ISBN")

# COMMAND ----------

# best-rated authors by year of publication and publishers
df_books_with_ratings = (
    df_books_with_ratings
    .groupBy("Year-Of-Publication", "Publisher", "Book-Author")
    .agg(avg("Book-Rating").alias("Book-Rating"))
)

# COMMAND ----------

df_books_with_ratings.createOrReplaceTempView("temp_view_avg_book_rating")
spark.sql("create table average_book_rating as select * from temp_view_avg_book_rating");
