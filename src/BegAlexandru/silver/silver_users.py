# Databricks notebook source
from pyspark.sql.functions import col, split, when, decode

# COMMAND ----------

# MAGIC %sql
# MAGIC use alexandru_beg_books

# COMMAND ----------

user_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)

# COMMAND ----------

# Cleaning the data from bronze users
users_df = (
    spark.readStream.table("bronze_users")
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

user_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + 'BegAlex_Books/silver/users'
)

# COMMAND ----------

(
    users_df
    .writeStream
    .format("delta")
    .option("checkpointLocation",
            "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_users_checkpoint/")
    .option("path", user_output_path)
    .outputMode("append")
    .table("silver_users")
)
