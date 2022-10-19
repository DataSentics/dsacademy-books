# Databricks notebook source
# Function to load the data from the azure storage

# COMMAND ----------

def auto_loader(data_source, source_format, checkpoint_directory, delimiter=';'):
    if source_format == 'json':
        query = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("cloudFiles.schemaLocation", checkpoint_directory)
            .load(data_source)
        )
    else:
        query = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("cloudFiles.schemaLocation", checkpoint_directory)
            .option("delimiter", delimiter)
            .load(data_source)
        )
    return query

# COMMAND ----------

# Creating a function to write into the azure storage account with a given dataframe, checkpoint path,
# the output path to the storage and to create a table with the table_name

# COMMAND ----------

def write_stream_azure_append(df, checkpoint, output_path, table_name):
    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("path", output_path)
        .trigger(availableNow=True)
        .outputMode("append")
        .table(table_name)
    )
    return query
