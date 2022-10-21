# Databricks notebook source
# notebook for stoping the streams

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
