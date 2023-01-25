# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

# Creating a dataframe containing the Bayesian rating ordered books

best_books_bayesian = spark.read.format('delta').load(f'{b.gold_path}/best_books_bayesian')

# COMMAND ----------

# Creating function to gather best books from certain period

def popular_books_years(first_year, last_year):
    return (best_books_bayesian
            .filter(f.col('year_of_publication') >= first_year)
            .filter(f.col('year_of_publication') <= last_year)
            .sort(f.col('bayesian_score').desc()))

# COMMAND ----------

# Best books in last 15 years

best_last_15_years = popular_books_years(2008, 2023)

# COMMAND ----------

# Saving best_last_15_years to path

best_last_15_years.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_last_15_years')
