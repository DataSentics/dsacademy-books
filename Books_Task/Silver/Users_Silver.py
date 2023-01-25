# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

# Importing necessary libraries

from pyspark.sql.window import Window
import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

# Creating a dataframe containing the users_bronze table

users_bronze = spark.table('users_bronze')

# COMMAND ----------

# Checking users_bronze data

display(users_bronze)
users_bronze.count()


# COMMAND ----------

# '''Generating location columns independently for city and country'''


# Location_Array creation

users_bronze_temp = users_bronze.select(f.split(users_bronze.Location, ', ', -1).alias('location_array'))


# Adding Location_Array to main user DF

w = Window.orderBy(f.monotonically_increasing_id())

users_bronze_index = users_bronze.withColumn("columnindex", f.row_number().over(w))
users_bronze_temp = users_bronze_temp.withColumn("columnindex", f.row_number().over(w))

users_bronze_array = (users_bronze_index
                      .join(users_bronze_temp,
                            users_bronze_index.columnindex
                            == users_bronze_temp.columnindex, 'inner')
                      .drop(users_bronze_temp.columnindex)
                      .drop(users_bronze_index.columnindex)
                      .drop(users_bronze_index.Location))


# Location_Array explode

new_users_bronze = (users_bronze_array
                    .withColumn("0", users_bronze_array.location_array[0])
                    .withColumn("1", users_bronze_array.location_array[1])
                    .withColumn("2", users_bronze_array.location_array[2])
                    .withColumn("3", users_bronze_array.location_array[3])
                    .withColumn("4", users_bronze_array.location_array[4])
                    .withColumn("5", users_bronze_array.location_array[5])
                    .withColumn("6", users_bronze_array.location_array[6])
                    .withColumn("7", users_bronze_array.location_array[7])
                    .withColumn("8", users_bronze_array.location_array[8]))


# Reversing the column order in order to use coalesce and get the country
# from the previously generated columns, resulting the new users_coalesced df

col_list = new_users_bronze.columns
reversed_cols = col_list[::-1]

users_reversed = (new_users_bronze
                  .select(reversed_cols))

users_coalesced = (users_reversed
                   .select("0", "Age", "User-ID",
                           f.coalesce("8", "7", "6", "5", "4", "3", "2", "1")
                           .alias("country")))

# COMMAND ----------

display(users_bronze_temp)

# COMMAND ----------

# Importing existing countries list to compare with dataset
# and assigning it to a list

country_list = (spark.read.option('header', True)
                .csv(f'{b.raw_files}/Country_list/countries.csv')
                .withColumn('name', f.lower('name')))

existing_countries = (country_list
                      .rdd.flatMap(lambda x: x).collect())

# COMMAND ----------

# Checking data

display(users_coalesced)
users_coalesced.printSchema()

# COMMAND ----------

# Cleaning the users_coalesced table

users_silver = (users_coalesced
                .filter(f.col("country").isin(existing_countries))
                .withColumnRenamed('0', 'city')
                .withColumnRenamed('User-ID', 'user_id')
                .withColumnRenamed('Age', 'age')
                .select('user_id', 'age', 'city', 'country')
                .na.replace('NULL', None)
                .na.replace({'n/a': None})
                .na.replace({'': None})
                .withColumn('user_id', f.col('user_id').cast('integer'))
                .withColumn('age', f.col('age').cast('integer'))
                .filter((f.col('age') >= 10) & (f.col('age') <= 120) | f.isnull('age')))

display(users_silver)
users_silver.count()

# COMMAND ----------

# Saving users_silver to path

users_silver.write.format('delta').mode('overwrite').save(b.users_silver_path)
