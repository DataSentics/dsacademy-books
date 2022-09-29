# Databricks notebook source
#notebook for cleaning the data
#used for books file

# COMMAND ----------

from pyspark.sql.functions import when,col

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

#path for reading the data 
reading_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AN_Books/books'
#path for writing back to the storage the cleaned data
writing_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata') + 'AN_Books/books_silver'


# COMMAND ----------

#saving the data into a dataframe
df=spark.read.parquet(reading_path)

# COMMAND ----------

#the col was full of 0 so i replaced them with null
df=df.withColumn("Year-Of-Publication",when(col("Year-Of-Publication")== 0 ,None).otherwise(col("Year-Of-Publication"))) 

# COMMAND ----------

display(df)

# COMMAND ----------

#registering the table in the metastore
df.write.mode("overwrite").saveAsTable("books_silver")

# COMMAND ----------

#writing it to the storage
df.write.parquet(writing_path, mode='overwrite')
