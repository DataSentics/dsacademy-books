# Databricks notebook source
#this notebook is for testing purposes only

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# COMMAND ----------

# MAGIC %sql
# MAGIC use ANiteanuBooks

# COMMAND ----------

user_path1 = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('alexandruniteanu')

# COMMAND ----------

my_schema = StructType([
  StructField('User-ID', StringType(), False),
  StructField('Location', StringType(), False),
  StructField('Age', StringType(), False)
])
# (spark
#     .readStream
#     .format('cloudFiles')
#     .option('cloudFiles.format', 'csv')
#     .option("cloudFiles.schemaLocation", "/FileStore/tables/checkpoint") 
#     .schema(my_schema)
#     .load(user_path1)
#     .writeStream
#     .option("checkpointLocation", "/FileStore/tables/checkpoint")   
#     .outputMode("append")
#     .toTable("user_raw"))

(spark.readStream.format("cloudFiles") 
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/FileStore/tables/checkpoint") 
    .schema(my_schema)
    .load(user_path1) 
    .writeStream 
    #.option("mergeSchema", "true") 
    .option("checkpointLocation", "/FileStore/tables/checkpoint") 
    .outputMode("append")
    .table("user_raw"))



# COMMAND ----------

# MAGIC %sql
# MAGIC drop table user_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_raw

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


