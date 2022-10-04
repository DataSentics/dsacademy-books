# Databricks notebook source
# getting the 2 tables, users and users_pii in the 3-rd normal form

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the pii_users table
reading_path_pii = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/pii_users_silver"
)
# path for reading the users table
reading_path_users = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_silver"
)
# path for writing back to the storage the joined table
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_3nf"
)

# COMMAND ----------

# saving the data into a dataframe
df_pii = spark.read.parquet(reading_path_pii)
df_users = spark.read.parquet(reading_path_users)

# COMMAND ----------

df_users = df_users.join(df_pii, ['User-ID'], how='inner')

# COMMAND ----------

# registering the table in the metastore
df_users.write.mode("overwrite").saveAsTable("users_3nf")

# COMMAND ----------

# writing it to the storage
df_users.write.parquet(writing_path, mode='overwrite')
