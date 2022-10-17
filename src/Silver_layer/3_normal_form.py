# Databricks notebook source
# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

df_users_pii = spark.readStream.table("silver_users_pii")

df_users = spark.readStream.table("silver_users")

# COMMAND ----------

# droped the col (_rescued_data) beacuse it was empty and preventing me from joining
new_users = df_users_pii.join(df_users, "User_ID").drop("_rescued_data")

# COMMAND ----------

(
    new_users.writeStream.format("delta")
    .option("checkpointLocation", users_3nf_checkpoint)
    .option("path", users_3nf_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_users_3nf")
)
