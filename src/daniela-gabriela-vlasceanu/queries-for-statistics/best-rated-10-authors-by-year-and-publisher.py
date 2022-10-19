# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df = spark.table("authors_pub_years_statistics")

# COMMAND ----------

# FUNCTION TO GET DISTINCT YEARS OF PUBLICATION FROM A DATAFRAME GIVEN AS PARAMETER
def get_years(df):
    years = (
        df.select("Year-Of-Publication")
        .rdd.map(lambda row: row[0])
        .distinct()
        .collect()
    )
    years.sort()
    return years

# COMMAND ----------

# FUNCTION TO GET DISTINCT PUBLISHERS FROM A DATAFRAME GIVEN AS PARAMETER
def get_publishers(df):
    publishers = df.select("Publisher").rdd.map(lambda row: row[0]).distinct().collect()
    return publishers

# COMMAND ----------

years = get_years(df)
publishers = get_publishers(df)

# COMMAND ----------

# CREATING THE PARAMETERS
dbutils.widgets.dropdown("Year", "2021", [str(x) for x in years])
dbutils.widgets.text("Publisher", "Lowell House")
# READING FROM DROPDOWN PARAMS
year = int(dbutils.widgets.get("Year"))

# COMMAND ----------

try:
    publisher = str(dbutils.widgets.get("Publisher"))
    if publisher not in publishers:
        raise ValueError()
except ValueError:
    print("Please enter a valid Publisher")

# COMMAND ----------

# FUNCTION FOR GETTING 10 MOST POPULAR AUTHOR BY YEAR AND PUBLISHER
def get_most_popular_authors(df, year, publisher):
    df_answer = (
        df.where(
            (f.col("Year-Of-Publication") == year) & (f.col("Publisher") == publisher)
        )
        .select("Book-Author", "Rating-for-statistics")
        .limit(10)
    )
    return df_answer

# COMMAND ----------

get_most_popular_authors(df, year, publisher).createOrReplaceTempView(
    "most_popular_authors"
)
spark.sql("SELECT * FROM most_popular_authors").show()
