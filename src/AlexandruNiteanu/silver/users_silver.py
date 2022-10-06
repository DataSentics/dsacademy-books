# Databricks notebook source
# notebook for cleaning the data
# used for user file

# COMMAND ----------

from pyspark.sql.functions import col, split, decode

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for writing the data after cleaning it
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_silver"
)

# COMMAND ----------

# saving the data into a dataframe
# splitting the col location into 3 different col and droping it after
# casting the col Age to integer(string before)
df = (
    spark.readStream.table("bronze_users")
    .withColumn("City", split(col("location"), ",").getItem(0))
    .withColumn("State", split(col("location"), ",").getItem(1))
    .withColumn("Country", split(col("location"), ",").getItem(2))
    .withColumn("Age", col("Age").cast("Integer"))
    .withColumn("city", decode(col("city"), "UTF-8"))
    .withColumn("state", decode(col("state"), "UTF-8"))
    .drop("location")
     )

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_users_checkpoint/",
).option("path", writing_path).outputMode(
    "append"
).table(
    "silver_users"
)
