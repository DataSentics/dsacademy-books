# Databricks notebook source
# MAGIC %sql
# MAGIC use ANiteanuBooks

# COMMAND ----------

books_rating_path1 ='abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu')# + 'BX-Book-Ratings.csv'
books_path1 = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu')# + 'BX-Books.csv'
user_path1 = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu')# +'BX-Users.csv'
pii_path1= 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('01rawdata')#+'books_crossing/users-pii.json'


# COMMAND ----------

from pyspark.sql.types import *
my_schema = StructType([
  StructField('User-ID', StringType(), False),
  StructField('Location', StringType(), False),
  StructField('Age', IntegerType(), False)
])
(spark
    .readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header',True)
    .schema(my_schema)
    .option("cloudFiles.schemaLocation", "/local_disk0/tmp/") 
    .load(user_path1)
    .writeStream
    .option("checkpointLocation","/local_disk0/tmp/")
    #.trigger(availableNow=True)
    .outputMode("append")
    .toTable("user_raw"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_raw

# COMMAND ----------


