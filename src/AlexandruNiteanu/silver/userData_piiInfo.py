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
    usersData_piiInfo_checkpoint
).option("path", usersData_piiInfo_path).trigger(availableNow=True).outputMode(
    "append"
).table(
    "usersData_piiInfo"
)
