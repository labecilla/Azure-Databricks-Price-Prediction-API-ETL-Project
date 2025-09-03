# Databricks notebook source
import pandas as pds

sourceDatePandasDF = pds.date_range(start='1/1/2023', end='31/12/2025')

sourceDateSparkDF = spark.createDataFrame(sourceDatePandasDF.to_frame())

display(sourceDateSparkDF)


# COMMAND ----------

sourceDateSparkDF.withColumnRenamed('0', 'calendar_date').createOrReplaceTempView("source_date_dim")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from source_date_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog pricing_analytics;
# MAGIC Truncate table  gold.reporting_dim_date_gold;
# MAGIC INSERT INTO gold.reporting_dim_date_gold (CALENDAR_DATE, DATE_ID, LAKEHOUSE_INSERTED_DATE, LAKEHOUSE_UPDATED_DATE)
# MAGIC SELECT 
# MAGIC     substring(calendar_date ,1,10) AS CALENDAR_DATE,
# MAGIC     date_format(calendar_date, 'yyyyMMdd' ) AS DATE_ID,
# MAGIC     current_timestamp() AS LAKEHOUSE_INSERTED_DATE,
# MAGIC     current_timestamp() AS LAKEHOUSE_UPDATED_DATE
# MAGIC from source_date_dim