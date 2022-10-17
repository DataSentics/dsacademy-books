# Databricks notebook source
def autoloader(source, formats, directory, delimiter):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formats)
        .option("cloudFiles.schemaLocation", directory)
        .option("delimiter", delimiter)
        .load(source)
    )
    return query
