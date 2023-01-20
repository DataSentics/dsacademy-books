# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.pathz as P
import pipelineutils.autoloader as A

# COMMAND ----------

A.autoload_to_table(P.book_ratings_path,
                    "book_ratings_bronze",
                    P.bronze_book_ratings_checkpoint_path,
                    "csv",
                    "latin1",
                    P.bronze_book_ratings_path,
                    ";"
                    )
