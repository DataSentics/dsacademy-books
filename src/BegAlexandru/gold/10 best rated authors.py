# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# 10 best-rated authors in total

# COMMAND ----------

# MAGIC %run ./Average_book_rating

# COMMAND ----------

display(joined_df)
