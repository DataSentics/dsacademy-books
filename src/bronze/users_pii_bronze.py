# Databricks notebook source
# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("radomirfabian")
    + "users-pii.json"
)

# COMMAND ----------

df_pii = (
    spark.read.option("header", "true")
    .option("delimiter", ";")
    .json(users_pii_path)
)

# COMMAND ----------

display(df_pii)

# COMMAND ----------

df_pii.write.mode('overwrite').saveAsTable("bronze_pii")

# COMMAND ----------

pii_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/pii'
)

# COMMAND ----------

df_pii.write.parquet(pii_output_path, mode='overwrite')

# COMMAND ----------


