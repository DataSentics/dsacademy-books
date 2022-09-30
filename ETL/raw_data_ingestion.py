# Databricks notebook source
from pyspark.sql.functions import col,avg,count,split

# COMMAND ----------

# MAGIC %sql
# MAGIC USE olteanedi_books;

# COMMAND ----------

#Here i created a function to avoid repetitive code in the next cells

def autoload(data_source, source_format, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("delimiter",";")
                  .load(data_source))
    return query

# COMMAND ----------

#Books raw data ingestion

books_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('oltean-edi') + '/raw-data/books/'
books_raw = autoload(books_path, "csv", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/books_raw_checkpoint/")

# COMMAND ----------

#Users raw data ingestion

user_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('oltean-edi') + '/raw-data/users/'
users_raw = autoload(user_path, "csv", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/users_raw_checkpoint/")

# COMMAND ----------

#Books rating raw data ingestion

books_rating_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('oltean-edi') + '/raw-data/ratings/'
books_rating_raw = autoload(books_rating_path, "csv", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/books_rating_raw_checkpoint/")

# COMMAND ----------

#Users pii raw data ingestion

pii_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('01rawdata') + 'books_crossing/'

pii_users = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/pii/")
                  .load(pii_path))

# COMMAND ----------

#Creating delta table for users pii

pii_users.writeStream.option("checkpointLocation", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/pii_raw_checkpoint/").toTable("pii_raw")

# COMMAND ----------

#Creating delta table for users

users_raw.writeStream.option("checkpointLocation", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/users_raw_checkpoint/").toTable("users_raw")

# COMMAND ----------

#Creating delta table for books

books_raw.writeStream.option("checkpointLocation", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/books_raw_checkpoint/").toTable("books_raw")

# COMMAND ----------

#Creating delta table for books ratings

books_rating_raw.writeStream.option("checkpointLocation", "/dbfs/user/cristian-eduard.oltean@datasentics.com/dbacademy/books_rating_raw_checkpoint/").toTable("books_rating_raw")
