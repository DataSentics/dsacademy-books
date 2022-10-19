# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

def get_years(df):
    years = (
        df.select("Year-Of-Publication")
        .where(f.col("Year-Of-Publication").isNotNull())
        .rdd.map(lambda row: row[0])
        .distinct()
        .collect()
    )
    years.sort()
    return years

# COMMAND ----------

books_df = spark.table("ratings_books")
years = get_years(books_df)
current_year = date.today().year

# COMMAND ----------

# CREATING THE PARAMETERS
dbutils.widgets.dropdown("From year", "2021", [str(x) for x in years])
dbutils.widgets.dropdown("To year", "2021", [str(x) for x in years])
dbutils.widgets.text("last N years", "1")
dbutils.widgets.dropdown("Choices", "period", ["period", "from&to"])
# READING FROM DROPDOWN PARAMS
to_year = int(dbutils.widgets.get("To year"))
from_year = int(dbutils.widgets.get("From year"))
choice = str(dbutils.widgets.get("Choices"))
max_num_of_period = 646

# COMMAND ----------

# READING FROM LAST N YEARS WIDGET AND HANDELING ERRORS:
try:
    period = int(dbutils.widgets.get("last N years"))
    if max_num_of_period <= period and period <= 1:
        raise ValueError()
except ValueError:
    print("Error, please enter numeric input from 1 to 646")

# COMMAND ----------

# FUNCTION FOR GETTING 10 MOST POPULAR BOOKS FROM YEAR -- TO YEAR
def get_most_popular_books_from_to(df, from_year, to_year):
    df_answer = (
        df.where(
            (f.col("Year-Of-Publication") >= from_year)
            & (f.col("Year-Of-Publication") <= to_year)
        )
        .sort(f.desc("Number-of-ratings"))
        .limit(10)
    )
    return df_answer

# COMMAND ----------

# FUNCTION FOR GETTING 10 MOST POPULAR BOOKS FROM A PERIOD OF TIME
def get_most_popular_books_period(df, period):
    df_answer = (
        df.where(
            (f.col("Year-Of-Publication") >= (current_year - period))
            & (f.col("Year-Of-Publication") <= current_year)
        )
        .sort(f.desc("Number-of-ratings"))
        .limit(10)
    )
    return df_answer

# COMMAND ----------

# READING THE RATINGS TABLE AND CALLING THE CORRECT FUNCTION

if choice == "period":
    get_most_popular_books_period(books_df, period).createOrReplaceTempView(
        "most_popular_books_period"
    )
    spark.sql("SELECT * FROM most_popular_books_period").show()
else:
    get_most_popular_books_from_to(
        books_df, from_year, to_year
    ).createOrReplaceTempView("most_popular_books_from_to")
    spark.sql("SELECT * FROM most_popular_books_from_to").show()
