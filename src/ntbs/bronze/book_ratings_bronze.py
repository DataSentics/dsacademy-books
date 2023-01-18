# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

autoload_to_table(book_ratings_path,
                  "book_ratings_bronze",
                  bronze_book_ratings_checkpoint_path,
                  "csv",
                  "latin1", bronze_book_ratings_path,
                  ";"
                  )
