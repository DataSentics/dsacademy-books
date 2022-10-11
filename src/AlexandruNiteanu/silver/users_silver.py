# Databricks notebook source
# notebook for cleaning the data
# used for user file

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, split

# COMMAND ----------

# splitting the col location into 3 different col and droping it after
# casting the col Age to integer(string before)
df_users = (
    spark.readStream.table("bronze_users")
    .withColumn("City", split(col("location"), ",").getItem(0))
    .withColumn("State", split(col("location"), ",").getItem(1))
    .withColumn("Country", split(col("location"), ",").getItem(2))
    .withColumn("Age", col("Age").cast("Integer"))
    .drop("location")
)

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    users_checkpoint
).option("path", users_path_cleansed).trigger(availableNow = True).outputMode(
    "append"
).table(
    "silver_users"
)
