# Databricks notebook source
# run WriteFunction using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../WriteFunction

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# the pii json is clean so it does not need anymore cleaning
df_pii = spark.readStream.table("bronze_pii")

# COMMAND ----------

WriteFunction(df_pii, 
              checkpoint_userspii_path, 
              pii_output_path, 
              "silver_pii",
             )
