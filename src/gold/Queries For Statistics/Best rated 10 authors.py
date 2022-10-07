# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df = spark.table("authors_ratings_for_statistics")
display(df.limit(10))
