# Databricks notebook source
# imports
import os

# COMMAND ----------

# usage of required schema

spark.sql("create database if not exists alexandru_checiches_gdc_books LOCATION '/dbacademy/alexandru.checiches@datasentics.com'")
spark.sql("use database alexandru_checiches_gdc_books")

# COMMAND ----------

# source data paths

az_path = "adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru"

book_ratings_path = f"abfss://01rawdata@{az_path}/book_ratings"
books_path = f"abfss://01rawdata@{az_path}/books"
users_path = f"abfss://01rawdata@{az_path}/users"
users_pii_path = f"abfss://01rawdata@{az_path}/users_pii"


# COMMAND ----------

checkpoint_path = "dbfs:/dbacademy/alexandru.checiches@datasentics.com/checkpoints"

# COMMAND ----------

# parsed data paths

bronze_book_ratings_path = f"abfss://02parseddata@{az_path}/bronze/book_ratings_bronze"
bronze_books_path = f"abfss://02parseddata@{az_path}/bronze/books_bronze"
bronze_users_path = f"abfss://02parseddata@{az_path}/bronze/users_bronze"
bronze_users_pii_path = f"abfss://02parseddata@{az_path}/bronze/users_pii_bronze"

# checkpoints

bronze_book_ratings_checkpoint_path = f"{checkpoint_path}/bronze_book_ratings_checkpoint"
bronze_books_checkpoint_path = f"{checkpoint_path}/bronze_books_checkpoint"
bronze_users_checkpoint_path = f"{checkpoint_path}/bronze_users_checkpoint"
bronze_users_pii_checkpoint_path = f"{checkpoint_path}/bronze_users_pii_checkpoint"

# COMMAND ----------

# cleansed data paths

silver_book_ratings_path = f"abfss://03cleanseddata@{az_path}/silver/book_ratings_silver"
silver_books_path = f"abfss://03cleanseddata@{az_path}/silver/books_silver"
silver_users_path = f"abfss://03cleanseddata@{az_path}/silver/users_silver"
silver_users_pii_path = f"abfss://03cleanseddata@{az_path}/silver/users_pii_silver"

# checkpoints
silver_book_ratings_checkpoint_path = f"{checkpoint_path}/silver_book_ratings_checkpoint"
silver_books_checkpoint_path = f"{checkpoint_path}/silver_books_checkpoint"
silver_users_checkpoint_path = f"{checkpoint_path}/silver_users_checkpoint"
silver_users_pii_checkpoint_path = f"{checkpoint_path}/silver_users_pii_checkpoint"

# COMMAND ----------

def autoload_to_table(data_source, table_name, checkpoint_directory, source_format, encoding, output_path, separator=";"):
    if source_format == "csv":
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("sep", separator)
                      .option("header", True)
                      .option("encoding", encoding)
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .option("path", output_path)
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
        
    elif source_format == "json":
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .option("path", output_path)
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
    return query
