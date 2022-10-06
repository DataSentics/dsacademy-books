# Databricks notebook source
# joining the table users and users_pii to get to a more normal form

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

pii_df = spark.sql("SELECT * FROM silver_pii")
users_df = spark.sql("SELECT * FROM silver_users")

# COMMAND ----------

# changing the name of the columns rescued data
pii_df = pii_df.withColumnRenamed("_rescued_data", "_rescued_data_pii")
users_df = users_df.withColumnRenamed("_rescued_data", "_rescued_data_users")
# joining the two tables, pii and users to a single table
users_df = users_df.join(pii_df, on='User-ID')

# COMMAND ----------

users_df.write.mode("overwrite").saveAsTable("3NF_users")

# COMMAND ----------

NF3_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/3NF/users_3nf"
)

# COMMAND ----------

users_df.write.parquet(NF3_path, mode='overwrite')
