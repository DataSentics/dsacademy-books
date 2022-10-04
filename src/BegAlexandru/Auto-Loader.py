# Databricks notebook source
def auto_loader(
    data_source, source_format, checkpoint_directory, delimiter, table_name
):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("delimiter", delimiter)
        .load(data_source)
        .writeStream.option("checkpointLocation", checkpoint_directory)
        .option("mergeSchema", "true")
        .table(table_name)
    )
    return query
