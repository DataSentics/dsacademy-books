# Databricks notebook source
spark.sql("create schema if not exists lucian_books_production;")
spark.sql("use lucian_books_production;")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

df_books_rating_schema = StructType([StructField('User-ID', IntegerType(), False),
                                     StructField('ISBN', StringType(), False),
                                     StructField('Book-Rating', IntegerType(), False)])


df_books_schema = StructType([StructField('ISBN', StringType(), True),
                              StructField('Book-Title', StringType(), True),
                              StructField('Book-Author', StringType(), True),
                              StructField('YearOf-Publication',
                                          IntegerType(), True),
                              StructField('Publisher', StringType(), True),
                              StructField('Image-URL-S', StringType(), True),
                              StructField('Image-URL-M', StringType(), True),
                              StructField('Image-URL-L', StringType(), True)])

df_users_schema = StructType([StructField('User-ID', IntegerType(), False),
                              StructField('Location', StringType(), True),
                              StructField('Age', IntegerType(), True)])

# COMMAND ----------

path_to_storage = 'abfss://01rawdata@adapeuacadlakeg2dev.dfs.core.windows.net/lucian-bidica'

path_to_parsed_storage = 'abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/lucian-bidica'

path_to_cleansed_storage = 'abfss://03cleanseddata@adapeuacadlakeg2dev.dfs.core.windows.net/lucian-bidica'

path_to_gold_storage = 'abfss://04golddata@adapeuacadlakeg2dev.dfs.core.windows.net/lucian-bidica'

ratings_source = f'{path_to_storage}/BX-Book-Ratings'
books_source = f'{path_to_storage}/BX-Books'
users_source = f'{path_to_storage}/BX-Users'
users_pii_source = f'{path_to_storage}/Users-pii'


ratings_checkpoint_raw = f'{path_to_storage}/ratings_raw_checkpoint'
books_checkpoint_raw = f'{path_to_storage}/book_raw_checkpoint'
users_checkpoint_raw = f'{path_to_storage}/users_raw_checkpoint'
users_pii_checkpoint_raw = f'{path_to_storage}/users_pii_raw_checkpoint'


ratings_parsed_path = f'{path_to_parsed_storage}/book_ratings_bronze'
books_parsed_path = f'{path_to_parsed_storage}/books_bronze'
users_parsed_path = f'{path_to_parsed_storage}/users_bronze'
users_pii_parsed_path = f'{path_to_parsed_storage}/users_pii_bronze'


ratings_cleansed_path = f'{path_to_cleansed_storage}/book_ratings_silver'
books_cleansed_path = f'{path_to_cleansed_storage}/books_silver'
users_cleansed_path = f'{path_to_cleansed_storage}/users_silver'
users_pii_cleansed_path = f'{path_to_cleansed_storage}/users_pii_silver'

answer_question = f'{path_to_gold_storage}'

# COMMAND ----------

def autoload_to_table(data_source, table_name, checkpoint_directory, path, source_format='csv', delimiter=';'):
    if source_format == 'csv':
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("sep", delimiter)
                      .option("encoding", "latin1")
                      .option("header", True)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("path", path)
                      .option("delta.columnMapping.mode", "name")
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
    elif source_format == 'json':
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .trigger(availableNow=True)
                      .option("path", path)
                      .outputMode("append")
                      .table(table_name))
    return query
