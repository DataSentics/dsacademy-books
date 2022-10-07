# Databricks notebook source
# MAGIC %sql
# MAGIC Use alexandru_beg_books

# COMMAND ----------

pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/pii"
)

# COMMAND ----------

pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/silver/pii"
)

# COMMAND ----------

# the pii json is clean so it does not need anymore cleaning
df_pii = spark.readStream.table("bronze_pii")

# COMMAND ----------

pii_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/silver/pii"
)

# COMMAND ----------

(
    df_pii
    .writeStream
    .format("delta")
    .option("checkpointLocation",
            "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_userspii_checkpoint/")
    .option("path", pii_output_path)
    .outputMode("append")
    .table("silver_pii")
)
