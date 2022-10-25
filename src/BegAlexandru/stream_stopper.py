# Databricks notebook source
# This will stop all active streams that are running

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
