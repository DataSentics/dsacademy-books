# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Creating a dataframe containing the users_pii_bronze
# and one containing users_bronze tables

users_pii_bronze = spark.table('users_pii_bronze')
users_silver = spark.read.format('delta').load(b.users_silver_path)

# COMMAND ----------

# Cleaning users_pii_bronze

users_pii_silver_temp = (users_pii_bronze
                         .withColumnRenamed('User-ID', 'user_id')
                         .withColumnRenamed('firstName', 'first_name')
                         .withColumnRenamed('lastName', 'last_name')
                         .withColumnRenamed('middleName', 'middle_name')
                         .select('user_id', 'first_name', 'middle_name', 'last_name', 'gender', 'ssn'))

# COMMAND ----------

# Joining the users_silver table with the users_pii table
# to create the final users_pii_silver table

users_pii_silver = (users_silver
                    .join(users_pii_silver_temp, 'user_id', 'inner')
                    .select('user_id', 'age', 'city', 'country', 'first_name',
                            'middle_name', 'last_name', 'gender', 'ssn')
                    .sort('user_id'))

display(users_pii_silver)

# COMMAND ----------

# Saving users_pii_silver to path

users_pii_silver.write.format('delta').mode('overwrite').save(b.users_pii_silver_path)
