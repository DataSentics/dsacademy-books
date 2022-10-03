# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 10 best-rated authors in total

# COMMAND ----------

new_df = (df2
          .select(["Book_Title","Publisher", "Book_Author", "Year_Of_Publication", "ISBN"])
         )

new_df2 = (df1
            .select("Book_Rating","ISBN")
          )

new_df = new_df.join(new_df2, "ISBN", how="inner")

new_df = (new_df
          .groupBy(["Book_Author", "Book_Rating"])
          
          .agg(mean("Book_Rating").alias("Rating_score"), count("Book_Rating").alias("Nr_of_ratings"))
         )

new_df = (new_df 
          .withColumn("Rating_score", col("Rating_score"))
          .sort(col("Rating_score").desc(), col("Nr_of_ratings").desc())
          .limit(10)
         )

display(new_df)
