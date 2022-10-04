# Databricks notebook source
from pyspark.sql.functions import col, split, when

# COMMAND ----------

# MAGIC %sql
# MAGIC use alexandru_beg_books

# COMMAND ----------

# cleaning the data from bronze users

# COMMAND ----------

user_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)

# COMMAND ----------

users_df = (
    spark.read.parquet(user_path)
    .withColumn("city", split(col("location"), ",").getItem(0))
    .withColumn("state", split(col("location"), ",").getItem(1))
    .withColumn("country", split(col("location"), ",").getItem(2))
    .withColumn("city", decode(col("city"), "UTF-8"))
    .withColumn("state", decode(col("state"), "UTF-8"))
    .withColumn("Age", when(col("Age") == "NULL", "unknown").otherwise(col("Age")).cast("Integer"))
    .withColumn("city", when(col("city") == "n/a", "unknown").otherwise(col("city")))
    .withColumn("state", when(col("state") == "n/a", "unknown").otherwise(col("state")))
    .fillna("unknown")
    .drop("location")
)

# COMMAND ----------

users_df.write.mode('overwrite').saveAsTable("silver_users")

# COMMAND ----------

user_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AlexB_Books/silver/users"
)

# COMMAND ----------

users_df.write.parquet(user_output_path, mode='overwrite')
