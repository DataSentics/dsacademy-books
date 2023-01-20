# Databricks notebook source
# MAGIC %md
# MAGIC # Delete databse and start over if necessary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE IF EXISTS filip_megiesan_books CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC # Create database if it doesn't already exist

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS filip_megiesan_books
             COMMENT 'This is Filips database'
             LOCATION '/dbfs/dbacademy/filip-mircea.megiesan@datasentics.com'""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Use database

# COMMAND ----------

spark.sql("USE filip_megiesan_books")
