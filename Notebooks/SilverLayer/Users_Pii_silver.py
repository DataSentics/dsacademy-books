# Databricks notebook source
# MAGIC %md
# MAGIC ##Owner : Anistoroaei Nicole-Laurian
# MAGIC ##Goal : create table that contains the id of user, gender and birthdate 

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import UtilitiesSilver.utilities as u
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# read the data from the blob storage and put it in a dataframe 
df_bronze_users_pii = spark.read.format("delta").load(u.users_pii_bronze_path)

# add a new column "UserID", and cast the columns as we need 
df_silver_users_pii = (df_bronze_users_pii
          .withColumn("UserID", (F.monotonically_increasing_id() + 1).cast(T.LongType()) )
          .withColumn("BirthDate", F.col("BirthDate").cast(T.DateType()))
          .withColumn("Gender", F.col("Gender").cast(T.StringType())))

# make a final dataframe just with the columns we need later for the task and save it in blob storage 
df_silver_users_pii_infos = df_silver_users_pii.select(F.col("UserID"), F.col("BirthDate"), F.col("Gender"))
df_silver_users_pii_infos.write.format('delta').mode('overwrite').save(u.users_pii_silver_path)

display(df_silver_users_pii)
