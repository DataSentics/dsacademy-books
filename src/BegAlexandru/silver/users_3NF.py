# Databricks notebook source
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../WriteFunction

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# joining the table users and users_pii to get to a more normal form

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

WriteFunction(users_df, 
              checkpoint_3nf_path, 
              NF3_output_path, 
              "3nf_users",
             )
