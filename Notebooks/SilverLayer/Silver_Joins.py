# Databricks notebook source
# MAGIC %md
# MAGIC #todo

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import pyspark.sql.functions as F
import utilities.utilities as u

# COMMAND ----------

df_books_silver = spark.read.format('delta').load(u.books_silver_path)
df_ratings_silver = spark.read.format('delta').load(u.ratings_silver_path)
df_users_silver = spark.read.format('delta').load(u.users_silver_path)
df_users_pii_silver = spark.read.format('delta').load(u.users_pii_silver_path)

# COMMAND ----------

df_joins = (df_users_silver
            .join(df_users_pii_silver, "UserID")
            .join(df_ratings_silver, "UserID")
            .join(df_books_silver, "ISBN"))
display(df_joins)

# COMMAND ----------

df_joins_silver = (df_joins
                   .withColumn("Age", F.floor(F.datediff(F.current_date(), F.col("birthDate")) / 365.25))
                   .drop("birthDate")
                   .drop("Year")
                   .drop("Publisher")
                   .withColumn("Location",F.col("Location.Country"))
                   .withColumn("Location", F.when(F.trim(F.col("Location")) == "N/a" , None)
                               .when(F.trim(F.col("Location")) == "", None)
                               .otherwise(F.col("Location")))
                   .drop("UserID"))
df_joins_silver.write.format('delta').mode('overwrite').save(u.joins_path)
