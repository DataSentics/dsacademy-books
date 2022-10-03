# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 10 best-rated authors by year of publication and publishers

# COMMAND ----------

new_df = (df2
          .select(["Book_Title","Publisher", "Book_Author", "Year_Of_Publication", "ISBN"])
         )

new_df2 = (df1
            .select("Book_Rating","ISBN")
          )

new_df = new_df.join(new_df2, "ISBN",how="inner")

new_df = (new_df
          .groupBy(["Publisher", "Year_Of_Publication"])
          .agg(avg("Book_Rating").alias("Rating_score"), count("Book_Rating").alias("Nr_of_ratings"))
         )

new_df = (new_df
          .sort(col("Rating_score").desc(), col("Nr_of_ratings").desc())
          .limit(10)
         )

display(new_df)
