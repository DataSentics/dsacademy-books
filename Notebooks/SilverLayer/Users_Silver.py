# Databricks notebook source
# MAGIC %md
# MAGIC todo

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import UtilitiesSilver.utilities as u
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

df_users_bronze = spark.read.format("delta").load(u.users_bronze_path).drop(F.col('_rescued_data'))

display(df_users_bronze.printSchema())
    
display(df_users_bronze)

# COMMAND ----------



df_users_silver = (df_users_bronze
    .withColumn("User-ID", F.col("User-ID").cast(T.IntegerType()))
    .withColumn("Age", F.when(F.col("Age") < 0, None).when(F.col("Age") > 115, None).otherwise(F.col("Age").cast(T.IntegerType())))
    .withColumn("Location", F.split(F.col("Location"), ","))
    .withColumn("City", F.initcap(F.col("Location").getItem(0)))
    .withColumn("State", F.initcap(F.col("Location").getItem(1)))
    .withColumn("Country", F.initcap(F.col("Location").getItem(2)))
    .withColumn("Location", F.struct("City", "State", "Country"))
    .withColumnRenamed("User-ID","UserID")
    .drop("City", "State", "Country")

    )
    
display(df_users_silver.printSchema())

display(df_users_silver)

# COMMAND ----------

df_users_silver.write.format('delta').mode('overwrite').save(u.users_silver_path)
