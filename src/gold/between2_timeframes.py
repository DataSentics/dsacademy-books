# Databricks notebook source
# 10 most popular books in any selected period - Create a python function which takes as
# its input a (pyspark) dataframe(/s) with all the required data and a period in some way 
# (e.g. last N years, or two parameters: from & to) and returns the answer as a dataframe.
# Show example results for the last 15 years.

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import date

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

df = spark.sql("select * from ANiteanuBooks.users_rating_books")

# COMMAND ----------

# get input from the user, 2 inputs, starting period , end period
def get_year():
    dbutils.widgets.text("period_start", "Enter the starting year")
    dbutils.widgets.text("period_end", "Enter the end of period")
    try:
        year_start = int(dbutils.widgets.get("period_start"))
        year_end = int(dbutils.widgets.get("period_end"))
    except:
        print("Wrong value, please enter 2 years")
    return year_start, year_end

# COMMAND ----------

# filter based on the input
def btw_2years_df(year_start, year_end):
    result_df = (
        df.filter(col("Year-Of-Publication") >= year_start)
        .filter(col("Year-Of-Publication") <= year_end)
        .groupBy("ISBN", "Book-Title")
        .count()
        .orderBy(col("count").desc())
        .withColumnRenamed("count", "No_Ratings")
        .limit(10)
    )
    return result_df

# COMMAND ----------

# function to get the results
def get_result(df, year_start, year_end):
    df = btw_2years_df(year_start, year_end)
    return df

# COMMAND ----------

# using the functions created above to get the input from user and display the result
year_start, year_end = get_year()
result_df = btw_2years_df(year_start, year_end)
display(result_df)
