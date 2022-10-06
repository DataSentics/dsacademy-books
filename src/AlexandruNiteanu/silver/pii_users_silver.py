# Databricks notebook source
# notebook for cleaning the data
# used for pii_users file

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for writing back to the storage the cleaned data
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/pii_users_silver"
)

# COMMAND ----------

# saving the data into a dataframe
df = (
    spark.readStream.table("bronze_users_pii"))

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_piiusers_checkpoint/",
).option("path", writing_path).outputMode(
    "append"
).table(
    "silver_users_pii"
)
