# Databricks notebook source
# getting the 2 tables, users and users_pii in the 3-rd normal form

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii = spark.readStream.table("silver_users_pii")
df_users = spark.readStream.table("silver_users")

# COMMAND ----------

df_users = df_users.withColumnRenamed("_rescued_data", "_rescued_data_users")
df_pii = df_pii.withColumnRenamed("_rescued_data", "_rescued_data_pii")

# COMMAND ----------

df_users = df_users.join(df_pii, ['User-ID'], how='inner')

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}usersData_piiInfo_checkpoint/"
).option("path", f"{storage}".format("03cleanseddata")
    + "AN_Books/users_3nf").trigger(availableNow=True).outputMode(
    "append"
).table(
    "usersData_piiInfo"
)
