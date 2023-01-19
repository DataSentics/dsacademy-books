# Databricks notebook source
# MAGIC %run ../init_setup 

# COMMAND ----------

silver_df_users = (spark
                   .table('bronze_users')
                   .withColumn('User-ID', f.col('User-ID').cast('integer'))
                   .withColumn('Age', f.col('Age').cast('integer'))
                   .withColumn('Location', f.initcap(f.col('Location')))
                   .withColumn('City', f.trim(f.split(f.col('Location'), ',').getItem(0)))
                   .withColumn('District', f.trim(f.split(f.col('Location'), ',').getItem(1)))
                   .withColumn('Country', f.trim(f.split(f.col('Location'), ',').getItem(2)))                    
                   .withColumn('City', f.when((f.col('City') == 'N/a') | (f.col('City') == ''), None).otherwise(f.col('City')))
                   .withColumn('District', f.when((f.col('District') == 'N/a') | (f.col('District') == ''), None).otherwise(f.col('District')))
                   .withColumn('Country', f.when((f.col('Country') == 'N/a') | (f.col('Country') == ''), None).otherwise(f.col('Country')))
                   .drop('Location')
                   .withColumnRenamed('_rescued_data', '_rescued_data_users')
                  )

# COMMAND ----------

silver_df_users_pii = (spark
                       .table('bronze_users_pii')
                       .withColumn('User-ID', f.col('User-ID').cast('integer'))
                       .withColumn('First-Name', f.trim(f.initcap(f.lower(f.col('firstName')))))
                       .withColumn('Last-Name', f.trim(f.initcap(f.lower(f.col('lastName')))))
                       .withColumn('Middle-Name', f.trim(f.initcap(f.lower(f.col('middleName')))))
                       .withColumn('Gender', f.trim(f.upper(f.col('gender'))))
                       .withColumnRenamed('_rescued_data', '_rescued_data_users_pii')
                       .drop('firstName', 'lastName', 'middleName')
                      )

# COMMAND ----------

silver_df_users = (silver_df_users
                   .join(silver_df_users_pii, 'User-ID', 'inner')
)

# COMMAND ----------

display(silver_df_users)

# COMMAND ----------

silver_df_users.write.format('delta').mode('overwrite').option('path', silver_users_path).saveAsTable('silver_users')
