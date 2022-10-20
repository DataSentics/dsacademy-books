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
    df.where(f.col("Book_Rating").isNotNull())
    .where(
        (f.col("Year_Of_Publication") > 2000)
        & (f.col("Year_Of_Publication") <= current_year)
    )
    .groupBy("User_ID", "FullName", "location_country")
    .agg(f.count("Book_Rating").alias("Number_of_ratings"))
    .sort(f.desc("Number_of_ratings"))
    .limit(1)
)

# COMMAND ----------

df_new.createOrReplaceTempView("user_who_rated_the_most_books")
spark.sql("SELECT * FROM user_who_rated_the_most_books").show()
