# Databricks notebook source
from pyspark.sql.functions import col, concat, avg, lit

# COMMAND ----------

# A new analytical query: What is the average book rating per gender (men vs. women)
# and per age group (age groups are: 0-10, 11-20, 21-30...)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

books_df = spark.table("silver_books")
ratings_df = spark.table("silver_ratings")
users_df = spark.table("3nf_users")

# COMMAND ----------

# rename the columns _rescued_data
ratings_df = ratings_df.withColumnRenamed("_rescued_data", "_rescued_data_ratings")
books_df = books_df.withColumnRenamed("_rescued_data", "_rescued_data_books")
# join all data into one single dataframe
Interval_of_age = ratings_df.join(books_df, on='ISBN').join(users_df, on='User-ID')

# COMMAND ----------

Interval_of_age = (
    Interval_of_age.withColumn("Interval", col("Age") - (col("Age") % 10))
    .withColumn(
        "Interval", concat(col("Interval") + 1, lit(" - "), col("Interval") + 10)
    )
    .groupBy("Interval", "gender")
    .agg(avg("Book-Rating").alias("Average_Book_Rating"))
)

# COMMAND ----------

Interval_of_age.createOrReplaceTempView("average_rating_by_gender_interval")
