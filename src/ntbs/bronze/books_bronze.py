# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.pathz as P

# COMMAND ----------

autoload_to_table(P.books_path,
                  "books_bronze",
                  P.bronze_books_checkpoint_path,
                  "csv",
                  "latin1",
                  P.bronze_books_path,
                  ";")
