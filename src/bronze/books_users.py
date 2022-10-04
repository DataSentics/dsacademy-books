# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS radomirfabian_books

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE radomirfabian_books

# COMMAND ----------

users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("radomirfabian")
    + "BX-Users.csv"
)

# COMMAND ----------

df_users = (
    spark.read
    .option("encoding", "UTF-8")
    .option("charset", "iso-8859-1")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(users_path)
)


# COMMAND ----------

display(df_users)

# COMMAND ----------

df_users.write.mode('overwrite').saveAsTable("bronze_users")

# COMMAND ----------

users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/users'
)

# COMMAND ----------

df_users.write.parquet(users_output_path, mode='overwrite')

# COMMAND ----------


