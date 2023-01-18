# Databricks notebook source
# usage of required schema

spark.sql("create database if not exists alexandru_checiches_books")
spark.sql("use database alexandru_checiches_books")

# COMMAND ----------

# required libs

import os
from pyspark.sql import functions as f
from pyspark.sql import types as t

# COMMAND ----------

#source data paths

raw_az_ext_path = "abfss://01rawdata@adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru"

book_ratings_path = os.path.join(raw_az_ext_path, "BX-Book-Ratings.csv")
books_path = os.path.join(raw_az_ext_path, "BX-Books.csv")
users_path = os.path.join(raw_az_ext_path, "BX-Users.csv")
users_pii_path = os.path.join(raw_az_ext_path, "Users-pii.json")

# COMMAND ----------

# parsed data paths

parsed_az_ext_path = "abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru/bronze"

bronze_book_ratings_path = os.path.join(parsed_az_ext_path, "book_ratings_bronze")
bronze_books_path = os.path.join(parsed_az_ext_path, "books_bronze")
bronze_users_path = os.path.join(parsed_az_ext_path, "users_bronze")
bronze_users_pii_path = os.path.join(parsed_az_ext_path, "users_pii_bronze")

# COMMAND ----------

#cleansed data paths

cleansed_az_ext_path = "abfss://03cleanseddata@adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru/silver"

silver_book_ratings_path = os.path.join(cleansed_az_ext_path, "book_ratings_silver")
silver_books_path = os.path.join(cleansed_az_ext_path, "books_silver")
silver_users_path = os.path.join(cleansed_az_ext_path, "users_silver")
silver_users_pii_path = os.path.join(cleansed_az_ext_path, "users_pii_silver")

# COMMAND ----------

# gold data paths

gold_az_ext_path = "abfss://04golddata@adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru/gold"
