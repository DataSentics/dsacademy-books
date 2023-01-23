# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

autoload_to_table(books_source, "books_bronze", books_checkpoint_raw,
                  books_parsed_path, source_format='csv', delimiter=';')
