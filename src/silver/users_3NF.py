# Databricks notebook source
# MAGIC %run ../includes/includes_silver

# COMMAND ----------

# Joining tables user and users_pii to assemble a normal form

# COMMAND ----------

pii_df = spark.readStream.table("silver_pii")
users_df = spark.readStream.table("silver_users")

# COMMAND ----------

# changing the name of the columns rescued data
pii_df = pii_df.withColumnRenamed("_rescued_data", "_rescued_data_pii")
users_df = users_df.withColumnRenamed("_rescued_data", "_rescued_data_users")
# joining the two tables, pii and users to a single table
users_df = users_df.join(pii_df, on='User-ID')

# COMMAND ----------

(
    users_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_3nf_path)
    .option("path", normalizedform_output_path)
    .trigger(availableNow=True)
    .option("overwriteSchema", "true")
    .outputMode("append")
    .table("3nf_users")
)