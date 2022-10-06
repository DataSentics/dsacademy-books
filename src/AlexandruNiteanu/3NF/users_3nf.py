# Databricks notebook source
# getting the 2 tables, users and users_pii in the 3-rd normal form

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for writing back to the storage the joined table
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_3nf"
)

# COMMAND ----------

# saving the data into a dataframe
df_pii = spark.readStream.table("silver_users_pii")
df_users = spark.readStream.table("silver_users")

# COMMAND ----------

df_users = df_users.join(df_pii, ['User-ID'], how='inner').drop("_rescued_data")

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/3NF_users_checkpoint/",
).option("path", writing_path).outputMode(
    "append"
).table(
    "3NF_users"
)
