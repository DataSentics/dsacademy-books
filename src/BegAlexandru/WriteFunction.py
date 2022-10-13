# Databricks notebook source
# Creating a writeStream function

# COMMAND ----------

def WriteFunction(df, checkpoint, output_path, table_name):
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
