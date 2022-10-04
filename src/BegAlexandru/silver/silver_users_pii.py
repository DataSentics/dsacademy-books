# Databricks notebook source
# MAGIC %sql
# MAGIC Use alexandru_beg_books

# COMMAND ----------

pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/pii"
)

# COMMAND ----------

# the pii json is clean so it does not need anymore cleaning

# COMMAND ----------

pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AlexB_Books/silver/pii"
)

# COMMAND ----------

df_pii = spark.read.parquet(pii_path)

# COMMAND ----------

df_pii.write.mode('overwrite').saveAsTable("silver_pii")

# COMMAND ----------

pii_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AlexB_Books/silver/pii"
)

# COMMAND ----------

df_pii.write.parquet(pii_output_path, mode='overwrite')
