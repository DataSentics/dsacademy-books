# Databricks notebook source
# This will stop all active streams that are running

# COMMAND ----------

for streams in spark.stream.active:
    streams.stop()
