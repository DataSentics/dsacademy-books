# Databricks notebook source
dbutils.notebook.run("/Repos/andrei-cosmin.tugmeanu@datasentics.com/andrei-tugmeanu-dsacademy-books/Bronze_layer", 60)

# COMMAND ----------

df = spark.sql(f"select * from {vw_books}")

# COMMAND ----------

df_book_ratings = (df_book_ratings
       .withColumnRenamed("User-ID","User_ID")
       .withColumnRenamed("Book-Rating","Book_Rating")
       .withColumn("Book_Rating", col("Book_Rating").cast("Integer"))
      )

# COMMAND ----------

df_books = (df_books
       .withColumnRenamed("Book-Title", "Book_Title")
       .withColumnRenamed("Book-Author", "Book_Author")
       .withColumnRenamed("Year-Of-Publication", "Year_Of_Publication")
       .withColumnRenamed("Image-URL-S", "Image_RL_S")
       .withColumnRenamed("Image-URL-M", "Image_URL_M")
       .withColumnRenamed("Image-URL-L", "Image_URL_L")
       .withColumn("Year_Of_Publication", when(col("Year_Of_Publication") == "0", "Unknown").otherwise(col("Year_Of_Publication")))
      )

# COMMAND ----------

df_users = (df_users
       .withColumnRenamed("User-ID", "User_ID")
       .withColumn("Age", when(col("Age") == "NULL", "Unknown").otherwise(col("Age")))
       .withColumn("City", split(col("Location"),",").getItem(0))
       .withColumn("State", split(col("Location"),",").getItem(1))
       .withColumn("Country", split(col("Location"),",").getItem(2))
       .drop("Location")
       .withColumn("State", when(col("State") == " ", "Unknown").otherwise(col("State")))
       .withColumn("Country", when(col("Country") == "", "Unknown").otherwise(col("Country")))
       .withColumn("City", when(col("City") == " ", "Unknown").otherwise(col("City")))
      )

# COMMAND ----------


