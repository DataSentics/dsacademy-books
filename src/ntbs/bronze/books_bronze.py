# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

autoload_to_table(books_path, "books_bronze", bronze_books_checkpoint_path, "csv", "latin1", bronze_books_path, ";")
