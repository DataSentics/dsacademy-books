# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users = spark.readStream.table("users_silver")
df_users_pii = spark.readStream.table("users_pii_silver")

df_u = df_users.drop(f.col("_rescued_data"))
df_u_p = df_users_pii.drop(f.col("_rescued_data"))

# COMMAND ----------

df_users_joined = df_u.join(df_u_p, "User-ID")

# COMMAND ----------

df_users_joined.createOrReplaceTempView("users_joined_pii_tempView")

# COMMAND ----------

users_path_upload_3nf = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/3NF/users"
)

# COMMAND ----------

(
    spark.table("users_joined_pii_tempView")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_users_joined_pii_checkpoint/",
    )
    .option("path", users_path_upload_3nf)
    .outputMode("append")
    .table("users_joined_pii_silver")
)
