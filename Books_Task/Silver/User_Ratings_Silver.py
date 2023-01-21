# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adapeuacadlakeg2dev.dfs.core.windows.net",
               "wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg==")

# COMMAND ----------

# Importing the users_pii table and the ratings table
# in order to create a new dataframe by joining the two

users_pii = spark.read.format('delta').load(users_pii_silver_path)
ratings = spark.read.format('delta').load(ratings_silver_path)

# COMMAND ----------

# Joining the two tables

user_ratings = (ratings
                .join(users_pii, 'User_ID', 'inner'))

display(user_ratings)

# COMMAND ----------

# Saving user_ratings to path

user_ratings.write.format('delta').mode('overwrite').save(f'{silver_files}/User_Ratings')
