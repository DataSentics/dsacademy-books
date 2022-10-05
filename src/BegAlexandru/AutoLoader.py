# Databricks notebook source
def auto_loader(data_source, source_format, checkpoint_directory, delimiter):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("delimiter", delimiter)
        .load(data_source)
    )
    return query
