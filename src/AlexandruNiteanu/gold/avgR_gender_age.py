# Databricks notebook source
# A new analytical query: What is the average book rating per gender
# (men vs. women) and per age group (age groups are: 0-10, 11-20, 21-30...)

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, avg, concat, lit

# COMMAND ----------

df_users = spark.table("usersData_piiInfo")
df_rating = spark.table("books_silver").withColumnRenamed("_rescued_data","_rescued_data_ratings")
df_books = spark.table("silver_ratings").withColumnRenamed("_rescued_data","_rescued_data_books")

# COMMAND ----------

df_rating_books = df_rating.join(df_books, on="ISBN")
df_users_rating_books = df_rating_books.join(df_users, on="User-ID")

# COMMAND ----------

interval = 10
df_result = (
    df_users_rating_books.withColumn("Interval", col("Age") - (col("Age") % 10))
    .withColumn(
        "Interval", concat(col("Interval") + 1, lit(" - "), col("Interval") + interval)
    )
    .groupBy("Interval", "gender")
    .agg(avg("Book-Rating").alias("Average_Book_Rating"))
)

# COMMAND ----------

df_result.createOrReplaceTempView("avg_rating_gender_age")

# COMMAND ----------

# registering the joined table
df_users_rating_books.write.mode("overwrite").saveAsTable("users_rating_books")
