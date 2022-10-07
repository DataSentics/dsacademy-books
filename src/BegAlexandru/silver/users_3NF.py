# Databricks notebook source
# joining the table users and users_pii to get to a more normal form

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

pii_df = spark.readStream.table("silver_pii")
users_df = spark.readStream.table("silver_users")

# COMMAND ----------

# changing the name of the columns rescued data
pii_df = pii_df.withColumnRenamed("_rescued_data", "_rescued_data_pii")
users_df = users_df.withColumnRenamed("_rescued_data", "_rescued_data_users")
# joining the two tables, pii and users to a single table
users_df = users_df.join(pii_df, on='User-ID')

# COMMAND ----------

NF3_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/3NF/users_3nf"
)

# COMMAND ----------

(
    users_df
    .writeStream
    .format("delta")
    .option("checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/nf3_checkpoint/")
    .option("path", NF3_output_path)
    .outputMode("append")
    .table("3nf_users")
)
