# Databricks notebook source
storage_account_name = 'adapeuacadlakeg2dev'

storage_account_access_key = 'wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg=='

blob_container = '01rawdata'

# COMMAND ----------

file_location = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/books_crossing/ratings_raw/BX-Book-Ratings.csv"

file_type = "csv"

# COMMAND ----------

spark.conf.set(

  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",

  storage_account_access_key)

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option('header','True').option('sep',';').load(file_location)

# COMMAND ----------

display(df)

# COMMAND ----------


