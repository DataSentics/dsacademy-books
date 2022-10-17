# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

users_pii_path_download = f"{azure_storage}Users-Pii/".format(
    "danielavlasceanu-gdc-final-task"
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option(
        "cloudFiles.schemaLocation",
        f"{working_dir}daniela_users_pii_r_checkpoint/",
    )
    .option("delimiter", ",")
    .load(users_pii_path_download)
    .createOrReplaceTempView("users_pii_raw_temp")
)

# COMMAND ----------

spark.sql(
    "CREATE OR REPLACE Temporary view users_pii_bronze_temp AS SELECT * FROM users_pii_raw_temp"
)

# COMMAND ----------

users_pii_path_upload = (
    f"{azure_storage}".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/users-pii"
)

# COMMAND ----------

(
    spark.table("users_pii_bronze_temp")
    .writeStream
    .trigger(availableNow=True)
    .format("parquet")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_users_pii_r_checkpoint/",
    )
    .option("path", users_pii_path_upload)
    .outputMode("append")
    .table("users_pii_bronze")
)
