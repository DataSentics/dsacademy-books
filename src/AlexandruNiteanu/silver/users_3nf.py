# Databricks notebook source
# getting the 2 tables, users and users_pii in the 3-rd normal form

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii = spark.readStream.table("silver_users_pii")
df_users = spark.readStream.table("silver_users")

# COMMAND ----------

# droped the col _rescued_data because it was empty and it was preventing me
# from joining the two tables since it was duplicated on both tables
# could've renamed it but since it was empty I think it is not needed
df_users = df_users.join(df_pii, ['User-ID'], how='inner').drop("_rescued_data")

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    users_3nf_checkpoint
).option("path", users_path_3nf).trigger(availableNow = True).outputMode(
    "append"
).table(
    "3NF_users"
)
