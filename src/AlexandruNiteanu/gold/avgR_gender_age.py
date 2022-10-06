# Databricks notebook source
# A new analytical query: What is the average book rating per gender
# (men vs. women) and per age group (age groups are: 0-10, 11-20, 21-30...)

# COMMAND ----------

from pyspark.sql.functions import col, avg, concat, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# getting the tables from the metastore and saving them into 3 dataframes
df_users = spark.sql("select * from users_3nf")
df_rating = spark.sql("select * from books_silver")
df_books = spark.sql("select * from books_rating_silver")

# COMMAND ----------

df = df_rating.join(df_books, on="ISBN")
df = df.join(df_users, on="User-ID")

# COMMAND ----------

interval = 10
df_result = (
    df.withColumn("Interval", col("Age") - (col("Age") % 10))
    .withColumn(
        "Interval", concat(col("Interval") + 1, lit(" - "), col("Interval") + interval)
    )
    .groupBy("Interval", "gender")
    .agg(avg("Book-Rating"))
)

# COMMAND ----------

# registering the joined table
df.write.mode("overwrite").saveAsTable("users_rating_books")
