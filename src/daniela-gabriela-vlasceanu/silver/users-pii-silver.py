# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users_pii = spark.readStream.table("users_pii_bronze")

df_users_pii_cleansed = (
    df_users_pii
    .withColumn(
        "FullName",
        f.concat(
            f.col("firstName"),
            f.lit(" "),
            f.col("middleName"),
            f.lit(" "),
            f.col("lastName"),
        )
    )
    .withColumnRenamed("User-ID","User_ID")
    .drop(f.col("_rescued_data"))
)

# COMMAND ----------

users_pii_path_upload_2 = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/users_pii"
)

# COMMAND ----------

df_users_pii_cleansed.createOrReplaceTempView("users_pii_silver_tempView")

# COMMAND ----------

(
    spark.table("users_pii_silver_tempView")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_users_pii_silver_checkpoint/",
    )
    .option("path", users_pii_path_upload_2)
    .outputMode("append")
    .table("users_pii_silver")
)
