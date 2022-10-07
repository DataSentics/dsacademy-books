# Databricks notebook source
from datetime import date
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

joined_df = spark.readStream.table("joined_books")

# COMMAND ----------

# Getting the choice from the user
def choice_of_years():
    dbutils.widgets.text("choice", "Enter the choice (1 or 2)")
    try:
        choice = int(dbutils.widgets.get("choice"))
    except UnboundLocalError:
        print("Wrong value imported please import a number 1 or 2")
    return choice

# COMMAND ----------

# Getting the last year or the period of time from the user
def year_period(choice):
    if (choice == 1):
        print("Please enter the number of years you want from now:")
        dbutils.widgets.text("year_o", "Enter the number of years")
        year_o = int(dbutils.widgets.get("year_o"))
        return year_o
    elif (choice == 2):
        print("Please enter the year period from start to end")
        dbutils.widgets.text("year1", "Enter the start period")
        year1 = int(dbutils.widgets.get("year1"))
        dbutils.widgets.text("year2", "Enter end period")
        year2 = int(dbutils.widgets.get("year2"))
        return year1, year2
    else:
        print("Choice not avaliable, try 1 or 2")

# COMMAND ----------

# Creating the dataframe with the 2 years
def dataframe_years(year_from, current_year):
    top_10_books_betw_2y = (
        joined_br_df.filter(col("Year-Of-Publication") >= year_from)
        .filter(col("Year-Of-Publication") <= current_year)
        .groupBy("ISBN", "Book-Title")
        .count()
        .orderBy(col("count").desc())
        .withColumnRenamed("count", "Number_of_ratings")
        .limit(10)
    )
    return top_10_books_betw_2y

# COMMAND ----------

# Calling the function based on the number of given years
def dataframe_result(joined_br_df, year1, year2=None):
    current_year = date.today().year
    if year2 is None:
        year_from = current_year - year1
        top_10_books_betw_2y = dataframe_years(year_from, current_year)
    else:
        top_10_books_betw_2y = dataframe_years(year1, year2)
    return top_10_books_betw_2y

# COMMAND ----------

# Calling all the functions
print("""1. Choice to get dataframe from last N years
2. Choice to get a dataframe from a period of time (x - y)
Please enter your choice (1 or 2):
""")
choice = choice_of_years()
print(f"Choice is {choice}\n")
if (choice == 1):
    year = year_period(choice)
    print(f"You chose to see the last {year} years")
    top_10_books_betw_2y = dataframe_result(joined_br_df, year)
else:
    year1, year2 = year_period(choice)
    print(f"The year periods are {year1} - {year2}")
    top_10_books_betw_2y = dataframe_result(joined_br_df, year1, year2)
print("\nThe new Dataframe looks like this:")
display(top_10_books_betw_2y)
