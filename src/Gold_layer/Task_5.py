# Databricks notebook source
# MAGIC %md
# MAGIC ####Average book rating per gender (men/women) and per age group (groups: 0-10, 11-20)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, floor, mean

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

df_users = spark.table("silver_users_3nf")

df_ratings = spark.table("silver_ratings")

# COMMAND ----------

df_users = df_users.join(df_ratings, "User_ID")

# COMMAND ----------

df_users = (
    df_users.withColumn("Age_categ", floor(col("Age") - (col("Age") % 10)))
    .withColumn(
        "Age_categ", concat((col("Age_categ") + 1), lit(" - "), (col("Age_categ") + 10))
    )
    .groupBy("Age_categ", "gender")
    .agg(mean("Book_Rating").alias("Avg_book_ratings"))
    .sort(col("Age_categ"))
)

display(df_users)
