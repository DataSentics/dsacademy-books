# Databricks notebook source
# MAGIC %md
# MAGIC # Delete database if needed (commented)

# COMMAND ----------

# MAGIC %md
# MAGIC # Import necessary packages

# COMMAND ----------

import os
import pyspark.sql.functions as f
import pyspark.sql.types as t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Start over
# MAGIC -- DROP DATABASE IF EXISTS filip_megiesan_books CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC # Create database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS filip_megiesan_books COMMENT 'This is Filips database' LOCATION '/dbfs/dbacademy/filip-mircea.megiesan@datasentics.com'")

# COMMAND ----------

# MAGIC %md
# MAGIC # Use database

# COMMAND ----------

spark.sql("USE filip_megiesan_books")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define azure dev storage account path

# COMMAND ----------

az_path = '@adapeuacadlakeg2dev.dfs.core.windows.net/filip_megiesan'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define raw paths

# COMMAND ----------

raw_az_path = f'abfss://01rawdata{az_path}'

raw_book_ratings_path = os.path.join(raw_az_path, 'book_ratings')
raw_books_path = os.path.join(raw_az_path, 'books/')
raw_users_path = os.path.join(raw_az_path, 'users/')
raw_users_pii_path = os.path.join(raw_az_path, 'users_pii/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define bronze paths

# COMMAND ----------

bronze_az_path = f'abfss://02parseddata{az_path}'

bronze_book_ratings_path = os.path.join(bronze_az_path, 'book_ratings')
bronze_books_path = os.path.join(bronze_az_path, 'books')
bronze_users_path = os.path.join(bronze_az_path, 'users')
bronze_users_pii_path = os.path.join(bronze_az_path, 'users_pii')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define silver paths

# COMMAND ----------

silver_az_path = f'abfss://03cleanseddata{az_path}'

silver_book_ratings_path = os.path.join(silver_az_path, 'book_ratings')
silver_books_path = os.path.join(silver_az_path, 'books')
silver_users_path = os.path.join(silver_az_path, 'users')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define gold paths

# COMMAND ----------

gold_az_path = f'abfss://04golddata{az_path}'

gold_top10_authors_path = os.path.join(gold_az_path, 'top10_authors')
gold_lost_publishers_path = os.path.join(gold_az_path, 'lost_publishers')
gold_top10_books_period_path = os.path.join(gold_az_path, 'top10_books_period')
gold_country_of_top_user_path = os.path.join(gold_az_path, 'country_of_top_user')
gold_top10_books_AGC_path = os.path.join(gold_az_path, 'top10_books_AGC')
gold_top10_authors_AGC_path = os.path.join(gold_az_path, 'top10_authors_AGC')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define checkpinting paths

# COMMAND ----------

checkpoint_bronze_book_ratings = os.path.join(bronze_az_path, 'checkpoint_bronze_book_ratings')
checkpoint_bronze_books = os.path.join(bronze_az_path, 'checkpoint_bronze_books')
checkpoint_bronze_users = os.path.join(bronze_az_path, 'checkpoint_bronze_users')
checkpoint_bronze_users_pii = os.path.join(bronze_az_path, 'checkpoint_bronze_users_pii')

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Incremental data loading method

# COMMAND ----------

def autoload_to_table(data_source, table_name, checkpoint_directory, source_format, encoding, output_path, separator=";"):
    """Reads cloud files incrementally with Autoloader and creates a table. Takes in the following parameter: data_source(string) >> raw data files cloud path;  table_name(string); checkpoint_directory(string) >> checkpointing directory path; source_format(string); encoding(string), separator(string)"""
         
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
                      .outputMode('append')
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .trigger(once=True)
                      .option('path', output_path)   
                      .table(table_name)
                )
    
    elif source_format == "json":
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .outputMode("append")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .trigger(once=True) 
                      .option('path', output_path)
                      .table(table_name)
                )
    
    return query
