# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

user_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "BX-Users.csv"
)

# COMMAND ----------

df_users = (
    spark.read.option("encoding", "ISO-8859-1")
    .option("header", "true")
    .option("delimiter", ";")
    .csv(user_path)
)

# COMMAND ----------

df_users.write.mode('overwrite').saveAsTable("bronze_users")

# COMMAND ----------

users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)

# COMMAND ----------

df_users.write.parquet(users_output_path, mode='overwrite')
