# Databricks notebook source
# usage of required schema

spark.sql("create database if not exists alexandru_checiches_books")
spark.sql("use database alexandru_checiches_books")

# COMMAND ----------

# source data paths

az_path = "adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru"

book_ratings_path = os.path.join("abfss://01rawdata@", az_path, "BX-Book-Ratings.csv")
books_path = os.path.join("abfss://01rawdata@", az_path, "BX-Books.csv")
users_path = os.path.join("abfss://01rawdata@", az_path, "BX-Users.csv")
users_pii_path = os.path.join("abfss://01rawdata@", az_path, "Users-pii.json")

# COMMAND ----------

# parsed data paths

bronze_book_ratings_path = os.path.join("abfss://02parseddata@", az_path, "/bronze/book_ratings_bronze")
bronze_books_path = os.path.join("abfss://02parseddata@", az_path, "/bronze/books_bronze")
bronze_users_path = os.path.join("abfss://02parseddata@", az_path, "/bronze/users_bronze")
bronze_users_pii_path = os.path.join("abfss://02parseddata@", az_path, "/bronze/users_pii_bronze")

# COMMAND ----------

# cleansed data paths

silver_book_ratings_path = os.path.join("abfss://03cleanseddata@", az_path, "/silver/book_ratings_silver")
silver_books_path = os.path.join("abfss://03cleanseddata@", az_path, "/silver/books_silver")
silver_users_path = os.path.join("abfss://03cleanseddata@", az_path, "/silver/users_silver")
silver_users_pii_path = os.path.join("abfss://03cleanseddata@", az_path, "/silver/users_pii_silver")
