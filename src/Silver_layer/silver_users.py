# Databricks notebook source
from pyspark.sql.function import col, when, split

# COMMAND ----------

# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

df_books_users = (
    spark.readStream.table("bronze_users")
    .withColumnRenamed("User-ID", "User_ID")
    .withColumn("Age", when(col("Age") == "NULL", "Unknown").otherwise(col("Age")))
    .withColumn("City", split(col("Location"), ",").getItem(0))
    .withColumn("State", split(col("Location"), ",").getItem(1))
    .withColumn("Country", split(col("Location"), ",").getItem(2))
    .drop("Location")
    .withColumn("State", when(col("State") == " ", "Unknown").otherwise(col("State")))
    .withColumn(
        "Country", when(col("Country") == "", "Unknown").otherwise(col("Country"))
    )
    .withColumn("City", when(col("City") == " ", "Unknown").otherwise(col("City")))
)

# COMMAND ----------

(
    df_books_users.writeStream.format("delta")
    .option("checkpointLocation", users_checkpoint)
    .option("path", users_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_users")
)
