# Databricks notebook source
# notebook for cleaning the data
# used for pii_users file

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the data
reading_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users_pii"
)
# path for writing back to the storage the cleaned data
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/pii_users_silver"
)

# COMMAND ----------

# saving the data into a dataframe
df = spark.read.parquet(reading_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# registering the table in the metastore
df.write.mode("overwrite").saveAsTable("pii_users_silver")

# COMMAND ----------

# writing it to the storage
df.write.parquet(writing_path, mode='overwrite')
