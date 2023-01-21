# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adapeuacadlakeg2dev.dfs.core.windows.net",
               "wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg==")

# COMMAND ----------

# Assigning the needed tables to dataframes

books = spark.read.format('delta').load(f'{silver_files}/books_silver')

# COMMAND ----------

# Creating the dataframe containing the publishers
# that were lost in time, sorted by oldest publication

lost_publishers = (books
                   .groupBy(f.col('Publisher')).max('Year_of_publication')
                   .sort(f.col('max(Year_of_publication)'))
                   .withColumnRenamed('max(Year_of_publication)', 'Latest_publication'))

display(lost_publishers)

# COMMAND ----------

# Saving lost_publishers to path

lost_publishers.write.format('delta').mode('overwrite').save(f'{gold_path}/lost_publishers')
