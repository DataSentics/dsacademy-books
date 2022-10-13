# Databricks notebook source
# This script will kill all the currently running streams and return human-readable output

# COMMAND ----------

def streamstopper():
    streamlist = spark.streams.active
    if len(streamlist) == 0:
        return "No streams are currently running."
    else:
        for i in streamlist:
            i.stop()
            print(f"Stream {i} has been killed.")
    print("Stream stopper has finished execution.")

# COMMAND ----------

streamstopper()
