# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users = spark.readStream.table("users_bronze")

df_users_cleansed = (
    df_users.withColumn("location-city", f.split(f.col("Location"), ",").getItem(0))
    .withColumn("location-state", f.split(f.col("Location"), ",").getItem(1))
    .withColumn("location-country", f.split(f.col("Location"), ",").getItem(2))
    .drop(f.col("Location"))
    .na.replace("N/a", None)
    .na.replace(" ", None)
    .na.replace("", None)
    .withColumn(
        "Age",
        (f.when(f.col("Age") == "NULL", None).otherwise(f.col("Age"))).cast("int"),
    )
    .withColumn("location-city", f.initcap(f.col("location-city")))
    .withColumn("location-state", f.initcap(f.col("location-state")))
    .withColumn("location-country", f.initcap(f.col("location-country")))
    .withColumn("User-ID", f.col("User-Id").cast("bigint"))
    .drop(f.col("_rescued_data"))

)

# COMMAND ----------

users_path_upload_2 = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/users"
)

# COMMAND ----------

df_users_cleansed.createOrReplaceTempView("users_silver_tempView")

# COMMAND ----------

(
    spark.table("users_silver_tempView")
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .option(
        "checkpointLocation",
        f"{working_dir}daniela_users_silver_checkpoint/",
    )
    .option("path", users_path_upload_2)
    .outputMode("append")
    .table("users_silver")
)
