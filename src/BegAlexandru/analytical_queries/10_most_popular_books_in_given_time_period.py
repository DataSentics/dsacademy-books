# Databricks notebook source
from pyspark.sql.functions import col, count

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

dbutils.widgets.text("period_start", "Enter the starting year")
dbutils.widgets.text("period_end", "Enter the end of period")

# COMMAND ----------

books_df = spark.table("silver_books")
ratings_df = spark.table("silver_ratings")
users_df = spark.table("3nf_users")

# COMMAND ----------

# rename the columns _rescued_data
ratings_df = ratings_df.withColumnRenamed("_rescued_data", "_rescued_data_ratings")
books_df = books_df.withColumnRenamed("_rescued_data", "_rescued_data_books")
# join all data into one single dataframe
df_ratings_with_books_and_users = ratings_df.join(books_df, on='ISBN').join(users_df, on='User-ID')

# COMMAND ----------

# get input from the user, 2 inputs, starting period , end period
year_start = int(dbutils.widgets.get("period_start"))
year_end = int(dbutils.widgets.get("period_end"))

# COMMAND ----------

# getting the data by filtering by year start and year end
def popular_books_from_interval(df, year_start, year_end):
    top_10_books_betw_2y = (
        df.filter(col("Year-Of-Publication") >= year_start)
        .filter(col("Year-Of-Publication") <= year_end)
        .groupBy("ISBN", "Book-Title")
        .agg(count("ISBN").alias("Number_of_Ratings"))
        .orderBy(col("Number_of_Ratings").desc())
        .limit(10)
    )
    return top_10_books_betw_2y

# COMMAND ----------

# using the function create above we call the function to filter the data
top_10_books_betw_2y = popular_books_from_interval(df_ratings_with_books_and_users, year_start, year_end)
top_10_books_betw_2y.createOrReplaceTempView("top_10_books_betw_2y")
