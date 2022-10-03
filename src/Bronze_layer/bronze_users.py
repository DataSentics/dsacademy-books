# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

users_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('andreitugmeanu') + 'BX-Users.csv'

# COMMAND ----------

df_users = (spark
            .read
            .format("csv")
            .option("encoding", "ISO-8859-1")
            .option("header", "true")
            .option("sep", ";")
            .load(users_path)
      )

# COMMAND ----------

df_users.write.mode('overwrite').saveAsTable("bronze_users")

# COMMAND ----------

books_output_path = ('abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'AT_books/Bronze/books_users')

# COMMAND ----------

df_users.write.parquet(books_output_path, mode='overwrite')

# COMMAND ----------


