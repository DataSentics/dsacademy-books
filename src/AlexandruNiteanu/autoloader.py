# Databricks notebook source
# function for incrementally loading the data

# COMMAND ----------

# function for ingesting the data

def autoload(data_source, source_format, checkpoint_directory, header_opt = None, delimiter = None, encode = None):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("delimiter", delimiter)
        .load(data_source)
    )
    return query
