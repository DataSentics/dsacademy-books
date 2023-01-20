# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %md
# MAGIC # Run initial setup

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Autoload data

# COMMAND ----------

autoload_to_table(m.raw_users_pii_path,
                  'bronze_users_pii',
                  m.checkpoint_bronze_users_pii,
                  'json', 'latin1',
                  m.bronze_users_pii_path,
                  separator=";")
