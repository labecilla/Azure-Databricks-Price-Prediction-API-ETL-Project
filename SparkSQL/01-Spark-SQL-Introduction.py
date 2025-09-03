# Databricks notebook source
# MAGIC %md
# MAGIC ##### Data File Path in DataLake Storage Account
# MAGIC
# MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ###### Spark Session Methods
# MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a>  **`read`**,**`write`**, **`createDataFrame`** , **`sql`** ,  **`table`**   
# MAGIC
# MAGIC ###### Dataframes To/From SQL Conversions
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html" target="_blank">DataFrame-SQLConversions</a> :**`createOrReplaceTempView`** ,**`spark.sql`**  ,**`createOrReplaceGlobalTempView`**

# COMMAND ----------

storageAccountKey='ju9IEFU6dmEvcYJyJMlBsx74EeJQlXIfkwny4Gfwfw0xCna8KaDe27tf3G+ON8lSlsysFyp8rE3N+AStZi8ZgA=='
spark.conf.set("fs.azure.account.key.adlsudadatalakehousedev.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsudadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'

# COMMAND ----------

sourceCSVFileDF = (spark
                   .read
                   .option("header","true")
                   .csv(sourceCSVFilePath)
)


# COMMAND ----------

sourceCSVFileDF.createOrReplaceTempView("daily_pricing_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_pricing_temp

# COMMAND ----------

spark.sql("SELECT * FROM daily_pricing_temp")

# COMMAND ----------

spark.table("daily_pricing_temp")

# COMMAND ----------

sourceCSVFileDF.createOrReplaceGlobalTempView("daily_pricing_global")