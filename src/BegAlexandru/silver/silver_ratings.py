# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../WriteFunction

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# cleaning the data from bronze rating

# COMMAND ----------

df_rating = (
    spark
    .readStream
    .table("bronze_ratings")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
)

# COMMAND ----------

WriteFunction(df_rating, 
              checkpoint_ratings_path, 
              rating_output_path, 
              "silver_ratings",
             )
