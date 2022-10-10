# Databricks notebook source
from datetime import date
from pyspark.sql.functions import col, count

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

books_df = spark.table("silver_books")
ratings_df = spark.table("silver_ratings")
users_df = spark.table("3nf_users")

# COMMAND ----------

# rename the columns _rescued_data
ratings_df = ratings_df.withColumnRenamed("_rescued_data", "_rescued_data_ratings")
books_df = books_df.withColumnRenamed("_rescued_data", "_rescued_data_books")
# join all data into one single dataframe
joined_df = ratings_df.join(books_df, on='ISBN').join(users_df, on='User-ID')

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
def dataframe_creation(year_from, current_year):
    top_10_books_betw_2y = (
        joined_df.filter(col("Year-Of-Publication") >= year_from)
        .filter(col("Year-Of-Publication") <= current_year)
        .groupBy("ISBN", "Book-Title")
        .agg(count("ISBN").alias("Rating_no"))
        .orderBy(col("Rating_no").desc())
        .limit(10)
    )
    return top_10_books_betw_2y

# COMMAND ----------

# Calling the function based on the number of given years
def dataframe_result(joined_df, year1, year2=None):
    current_year = date.today().year
    if year2 is None:
        year_from = current_year - year1
        top_10_books_betw_2y = dataframe_creation(year_from, current_year)
    else:
        top_10_books_betw_2y = dataframe_creation(year1, year2)
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
    top_10_books_betw_2y = dataframe_result(joined_df, year)
else:
    year1, year2 = year_period(choice)
    print(f"The year periods are {year1} - {year2}")
    top_10_books_betw_2y = dataframe_result(joined_df, year1, year2)
print("\nThe new Dataframe looks like this:")

# COMMAND ----------

top_10_books_betw_2y.createOrReplaceTempView("top_10_books_betw_2y")
