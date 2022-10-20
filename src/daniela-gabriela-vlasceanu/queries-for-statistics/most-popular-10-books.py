# Databricks notebook source
import pyspark.sql.functions as f
from datetime import date

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

def get_years(df):
    years = (
        df.select("Year_Of_Publication")
        .where(f.col("Year_Of_Publication").isNotNull())
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

# COMMAND ----------

# READING FROM DROPDOWN PARAMS
to_year = int(dbutils.widgets.get("To year"))
from_year = int(dbutils.widgets.get("From year"))
choice = str(dbutils.widgets.get("Choices"))

# COMMAND ----------

# ADDING SOME USEFUL VARIABLES FOR PERIOD OF YEARS POSSIBLE
max_year = int(max(years))
min_year = int(min(years)) 
max_num_of_period = max_year - min_year

# COMMAND ----------

# READING FROM LAST N YEARS WIDGET AND HANDELING ERRORS:
period = int(dbutils.widgets.get("last N years"))
if max_num_of_period <= period and period <= 1:
    dbutils.notebook.exit('Error, please enter numeric input from 1 to 646')

# COMMAND ----------

def get_most_popular_books_from_to(df, from_year, to_year):
    """ FUNCTION FOR GETTING 10 MOST POPULAR BOOKS FROM YEAR -- TO YEAR  """
    df_answer = (
        df.where(
            (f.col("Year_Of_Publication") >= from_year)
            & (f.col("Year_Of_Publication") <= to_year)
        )
        .sort(f.desc("Number_of_ratings"))
        .limit(10)
    )
    return df_answer

# COMMAND ----------

def get_most_popular_books_period(df, period):
    """ FUNCTION FOR GETTING 10 MOST POPULAR BOOKS FROM A PERIOD OF TIME """
    df_answer = (
        df.where(
            (f.col("Year_Of_Publication") >= (current_year - period))
            & (f.col("Year_Of_Publication") <= current_year)
        )
        .sort(f.desc("Number_of_ratings"))
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
