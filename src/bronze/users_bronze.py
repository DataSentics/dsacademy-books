# Databricks notebook source
#notebook for reading the data from the storage and write it as parquet
#used for BX-Users

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

#path for reading the csv
user_path_reading = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu') + 'BX-Users.csv'
#path for writing the csv as parquet
user_path_writing = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AN_Books/users'


# COMMAND ----------

#saving the csv into a df and getting rid of those special characters
df = spark.read.option("header", "true").option("nullValue", None).option("encoding", "iso8859-1").option("delimiter",";").csv(user_path_reading)


# COMMAND ----------

#registering the table in the metastore
df.write.mode("overwrite").saveAsTable("users_raw")


# COMMAND ----------

#writing it as parquet in the azure storage
df.write.parquet(user_path_writing, mode='overwrite')
