# Databricks notebook source
from pyspark.sql.functions import col, avg

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

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
joined_df = silver_books_df.join(silver_ratings_df, on="ISBN")

# COMMAND ----------

# best-rated authors by year of publication and publishers
joined_df = (
    joined_df
    .groupBy("Year-Of-Publication", "Publisher", "Book-Author")
    .agg(avg("Book-Rating").alias("Book-Rating"))
)

# COMMAND ----------

joined_df = (
    joined_df.orderBy(col("Book-Rating").desc())
)

# COMMAND ----------

(
    joined_df
    .write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("gold_book_rating_by_year_of_pub")
)
