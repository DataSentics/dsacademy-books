# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

users_path = f"{azure_storage}Users/".format(
    "danielavlasceanu-gdc-final-task"
)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        f"{working_dir}daniela_users_raw_checkpoint/",
    )
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(users_path)
    .createOrReplaceTempView("users_raw_temp")
)

# COMMAND ----------

spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW users_bronze_temp AS SELECT * FROM users_raw_temp"
)

# COMMAND ----------

users_path_upload = (
    f"{azure_storage}".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/users"
)

# COMMAND ----------

(
    spark.table("users_bronze_temp")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_users_raw_checkpoint/",
    )
    .option("path", users_path_upload)
    .outputMode("append")
    .table("users_bronze")
)
