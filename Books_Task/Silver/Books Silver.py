# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install isbnlib

# COMMAND ----------

# Importing ISBN library and creating UDF from it in order to be applied as a cleaning filter

from isbnlib import is_isbn10, is_isbn13

is_valid_isbn = udf(lambda x: is_isbn10(x) or is_isbn13(x), t.BooleanType())

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adapeuacadlakeg2dev.dfs.core.windows.net",
               "wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg==")

# COMMAND ----------

# Creating a dataframe containing the books_bronze table

books_bronze = spark.table('books_bronze')

# COMMAND ----------

# Displaying books_bronze

display(books_bronze)
books_bronze.count()

# COMMAND ----------

# Cleaning books_bronze

books_silver = (books_bronze
               .withColumnRenamed('Book-Title', 'Book_Title')
               .withColumnRenamed('Book-Author', 'Book_Author')
               .withColumnRenamed('Year-Of-Publication', 'Year_of_publication')
               .withColumn('ISBN', f.regexp_replace(f.col('ISBN'), '[^0-9X]', ''))
               .filter(is_valid_isbn(f.col("ISBN")))
               .withColumn('Book_Author', f.initcap
                           (f.trim(f.regexp_replace
                                   (f.col('Book_Author'),
                                    '(?<=[A-Za-z])\.(?=[A-Za-z])',
                                    '. '))))
               .withColumn('Book_Author', f.regexp_replace(f.col('Book_Author'), '&amp;', '&'))
               .withColumn('Book_Title', f.regexp_replace(f.col('Book_Title'), '&amp;', '&'))
               .withColumn('Publisher', f.regexp_replace(f.col('Publisher'), '&amp;', '&')) 
               .withColumn(('Year_of_publication'), f.col('Year_of_publication').cast('integer'))
               .withColumn("Year_of_publication", f.when(
                          (f.col("Year_of_publication") == 0) | (f.col("Year_of_publication") > 2023), None)
                          .otherwise(f.col("Year_of_publication")))
               .drop('Image-URL-S', 'Image-URL-M', 'Image-URL-L', '_rescued_data'))

# COMMAND ----------

# Displaying books_silver

display(books_silver)
books_silver.printSchema()

# COMMAND ----------

books_bronze_cleaning.count()

# COMMAND ----------

# Saving books_silver to path

books_silver.write.format('delta').mode('overwrite').save(books_silver_path)
