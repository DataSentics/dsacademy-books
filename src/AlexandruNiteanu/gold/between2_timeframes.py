# Databricks notebook source
# 10 most popular books in any selected period - Create a python function which takes as
# its input a (pyspark) dataframe(/s) with all the required data and a period in some way
# (e.g. last N years, or two parameters: from & to) and returns the answer as a dataframe.
# Show example results for the last 15 years.

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

dbutils.widgets.text("period_start", "Enter the starting year")
dbutils.widgets.text("period_end", "Enter the end of period")

# COMMAND ----------

df_users_rating_books = spark.table("users_rating_books")

# COMMAND ----------

# get input from the user, 2 inputs, starting period , end period
def get_years():
    try:
        year_start = int(dbutils.widgets.get("period_start"))
        year_end = int(dbutils.widgets.get("period_end"))
    except ValueError:
        print("Wrong value, please enter 2 years")
    return year_start, year_end

# COMMAND ----------

# filter based on the input
def popular_books_in_interval(df, year_start, year_end):
    result_df = (
        df_users_rating_books.filter(col("Year-Of-Publication") >= year_start)
        .filter(col("Year-Of-Publication") <= year_end)
        .groupBy("ISBN", "Book-Title")
        .agg(count("ISBN").alias("No_Ratings"))
        .orderBy(col("No_Ratings").desc())
        .limit(10)
    )
    return result_df

# COMMAND ----------

# using the functions created above to get the input from user and display the result
year_start, year_end = get_years()
result_df = popular_books_in_interval(df_users_rating_books, year_start, year_end)
result_df.createOrReplaceTempView("popular_books_btw_timeframes")
