# Databricks notebook source
# This script will kill all the currently running streams and return human-readable output

# COMMAND ----------

streamlist = spark.streams.active
if len(streamlist) == 0:
    print("No streams are currently running.")
else:
    for stream in streamlist:
        stream.stop()
        print(f"Stream {stream} has been killed.")
print("Stream killer has finished execution.")
