# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 10 most popular books in any selected (parameter period = 2years e.g.) period (last year, 2 years, ever)

# COMMAND ----------

from pyspark.sql.functions import col, avg, count
from datetime import date

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

# COMMAND ----------

df_books_rating = (spark.read.parquet(books_rating_path))

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books"
)

# COMMAND ----------

df_books = (spark.read.parquet(books_path))

# COMMAND ----------

def joinDataframes():
    aux_df = df_books.select(
        ["Book_Title", "Publisher", "Book_Author", "Year_Of_Publication", "ISBN"]
    )

    aux_df2 = df_books_rating.select("Book_Rating", "ISBN")

    joined_df = aux_df.join(aux_df2, "ISBN")
    return joined_df

# COMMAND ----------

def chooseYear():
    dbutils.widgets.text("pick", "Pick 1, 2 or 3")
    var_pick = int(dbutils.widgets.get("pick"))
    return var_pick

# COMMAND ----------

def yearPeriodAbove():
    print("Select the years from it and above:")
    dbutils.widgets.text("yearAbove", "Enter the number of years")
    varYearAbove = int(dbutils.widgets.get("yearAbove"))
    return varYearAbove

# COMMAND ----------

def yearPeriodBetweer():
    print("Select the border years")
    dbutils.widgets.text("year1", "Left border")
    varYear1 = int(dbutils.widgets.get("year1"))
    dbutils.widgets.text("year2", "Right border")
    varYear2 = int(dbutils.widgets.get("year2"))
    return varYear1, varYear2

# COMMAND ----------

def dataframeAbove(df, left_border):
    right_border = date.today().year
    df_above = (
        df.filter(
            (col("Year_Of_Publication") >= left_border)
            & (col("Year_Of_Publication") <= right_border)
        )
        .groupBy("Book_Title")
        .agg(count(col("Book_Title")).alias("Top_10_books_by_rating"))
        .sort(col("Top_10_books_by_rating").desc())
        .take(10)
    )
    return df_above

# COMMAND ----------

def dataframeBetweer(df, left_border, right_border):
    df_between = (
        df.filter(
            (col("Year_Of_Publication") >= left_border)
            & (col("Year_Of_Publication") <= right_border)
        )
        .groupBy("Book_Title")
        .agg(count(col("Book_Title")).alias("Top_10_books_by_rating"))
        .sort(col("Top_10_books_by_rating").desc())
        .take(10)
    )
    return df_between

# COMMAND ----------

def dataframeEver(df):
    df_ever = (
        df.groupBy("Book_Title")
        .agg(count(col("Book_Title")).alias("Top_10_books_by_rating"))
        .sort(col("Top_10_books_by_rating").desc())
        .take(10)
    )
    return df_ever

# COMMAND ----------

def main():
    joined_df = joinDataframes()
    print("Enter 1 for given year and above or 2 for between two years")
    pick = chooseYear()

    if pick == 1:
        year = yearPeriodAbove()
        display(dataframeAbove(joined_df, year))
    elif pick == 2:
        yearLeft, yearRight = yearPeriodBetweer()
        dataframeBetweer(joined_df, yearLeft, yearRight)
    elif pick == 3:
        dataframeEver(joined_df)
    else:
        return "Value not in boundries"

# COMMAND ----------

main()

# COMMAND ----------


