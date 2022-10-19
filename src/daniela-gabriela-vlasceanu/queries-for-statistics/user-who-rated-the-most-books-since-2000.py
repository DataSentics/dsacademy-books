# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df = spark.table("users_ratings")
current_year = date.today().year

# COMMAND ----------

df_new = (
    df.where(f.col("Book-Rating").isNotNull())
    .where(
        (f.col("Year-Of-Publication") > 2000)
        & (f.col("Year-Of-Publication") <= current_year)
    )
    .groupBy("User-ID", "FullName", "location_country")
    .agg(f.count("Book-Rating").alias("Number_of_ratings"))
    .sort(f.desc("Number_of_ratings"))
    .limit(1)
)
display(df_new)
