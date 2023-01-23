# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import pyspark.sql.functions as f
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %md
# MAGIC # Run initial setup

# COMMAND ----------

# MAGIC %run ../use_database

# COMMAND ----------

# MAGIC %md
# MAGIC # Answer question

# COMMAND ----------

df_gold_lost_publishers = (spark
                           .table("silver_books")
                           .groupBy("Publisher")
                           .agg(f.max(f.col("Year-Of-Publication")).alias("Last-Publication-Year"))
                           .filter(f.col("Last-Publication-Year") < 2000)
                           .sort(f.col("Last-Publication-Year")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(df_gold_lost_publishers, m.gold_lost_publishers_path, 'gold_lost_publishers')
