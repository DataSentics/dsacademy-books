# Databricks notebook source
from pyspark.sql.functions import col, split, when

# COMMAND ----------

# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../WriteFunction

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# Cleaning the data from bronze users
users_df = (
    spark.readStream.table("bronze_users")
    .withColumn("city", split(col("location"), ",").getItem(0))
    .withColumn("state", split(col("location"), ",").getItem(1))
    .withColumn("country", split(col("location"), ",").getItem(2))
    .withColumn("Age", when(col("Age") == "NULL", "unknown").otherwise(col("Age")).cast("Integer"))
    .withColumn("city", when(col("city") == "n/a", "unknown").otherwise(col("city")))
    .withColumn("state", when(col("state") == "n/a", "unknown").otherwise(col("state")))
    .fillna("unknown")
    .drop("location")
)

# COMMAND ----------

WriteFunction(users_df, 
              checkpoint_users_path, 
              user_output_path, 
              "silver_users",
             )
