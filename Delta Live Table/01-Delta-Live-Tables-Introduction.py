# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables
# MAGIC
# MAGIC Instead of defining your data transformations using a series of separate notebooks, you define sequence of proceesing steps using Delta Live Tables that the system should create and keep up to date. Delta Live Tables manages how your data is transformed based on queries you define for each processing step. We call this as declarative framework as when developing the code we are just declaring the Data Transformations and not running it. Entire process will be executed using the Delta Live Table Pipeline that automatically manages task orchestration, cluster management, monitoring, data quality, and error handling.
# MAGIC
# MAGIC Delta Live Tables Can be Implemented using 3 Type Of Delta Live Tables Datasets and see below table for their purpose and usage
# MAGIC
# MAGIC | Delta Live Table Dataset Types | Usage | Data Processing |
# MAGIC | --- | --- |--- |
# MAGIC |Streaming Tables| Configured to Ingest Continoulsy Changing Source Data  and Applying Incremental Transformation on the Source data| Each Record Prpcessed Only Once , Automatically Indentify New Source data |
# MAGIC |Materialized Views| Configured to source the data for multiple jobs/pipelines as updated once and reused multiple times| Records are processed as required to return the current state of Dataset |
# MAGIC |Views| Configured to Break complex queries to multiple simple queries| Records are processed Each Time the View is Queried |
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/delta-live-tables/index.html" target="_blank">Delta Live Tables Documentation</a>
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/delta-live-tables/python-dev.html" target="_blank">Delta Live Tables Python API</a>
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/delta-live-tables/sql-dev.html" target="_blank">Delta Live Tables SQL API</a>
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html" target="_blank">Load And Transform With Delta Live Tables</a>
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html" target="_blank">Delta Live Tables Auto Loader Configuration</a>
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/delta-live-tables/cdc.html" target="_blank">Change Data Capture(CDC) With Delta Live Tables</a>
# MAGIC
# MAGIC ***Source Stream Data Schema*** : "ARRIVAL_IN_TONNES double,DATETIME_OF_PRICING string ,MARKET_NAME string,MAXIMUM_PRICE double,MINIMUM_PRICE double,MODAL_PRICE double,ORIGIN string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,ROW_ID long,STATE_NAME string,VARIETY string,source_stream_load_datetime string"

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Create Delta Live Table Materialized View
# MAGIC
# MAGIC 1. Need to use Python Function Definition keyword def
# MAGIC 1. Function name will be used as Delta Live Table  By Default
# MAGIC 1. Delta Live Table names can be defined using the Property name . e.g @dlt.table(name = "table_name")
# MAGIC 1. Use Standard spark.read API to create delata files for the data in files
# MAGIC 1. Use Standard spark.sql API to create delata files for the data in tables
# MAGIC 1. We wont be able to use Magic commands inside the Delta Live Table Notebook and can use only one of the default programming language either Python Or SQL
# MAGIC 1. Enclose Spark commands inside return statement to create the Deltra Live Table as a output
# MAGIC 1. Delta Live Materialised views Completely Reloaded for Each Run

# COMMAND ----------

@dlt.table
def dlt_daily_pricing_materialized_view():
    return (
        spark
        .read
        .format("json")
        .schema("ARRIVAL_IN_TONNES double,DATETIME_OF_PRICING string ,MARKET_NAME string,MAXIMUM_PRICE double,MINIMUM_PRICE double,MODAL_PRICE double,ORIGIN string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,ROW_ID long,STATE_NAME string,VARIETY string,source_stream_load_datetime string")
        .load("abfss://bronze@adlsudalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Create Streaming Delta Live Table
# MAGIC
# MAGIC
# MAGIC 1. Use spark.readStream API for data Stored in files
# MAGIC 1. Use Standard spark.readStream.table API for data Stored in tables
# MAGIC 1. Specify the .format("cloudFiles") to enable AUTO LOADER in streaming table
# MAGIC 1. AutoLoader Enables Automatic Incremental Load

# COMMAND ----------

@dlt.table(name = "dlt_daily_pricing_streaming_table")
def create_streaming_table():
    return (
       spark
      .readStream
      .format("cloudFiles")
      .schema("ARRIVAL_IN_TONNES double,DATETIME_OF_PRICING string ,MARKET_NAME string,MAXIMUM_PRICE double,MINIMUM_PRICE double,MODAL_PRICE double,ORIGIN string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,ROW_ID long,STATE_NAME string,VARIETY string,source_stream_load_datetime string")
        .option("cloudFiles.format", "json")
        .load("abfss://bronze@adlsudadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3: Create Delta Live Table From Existing Delta Live Table
# MAGIC
# MAGIC 1. Use dlt.read_stream({deltalivetablename>}) to read existing Delta Live Streaming Table
# MAGIC 1. Spark API equivalent to dlt.read.stream() is spark.readStream.table() and please note spark API dont work for Delta Live Table Views at the time of recording
# MAGIC 1. Use dlt.read({deltalivetablename>}) to read existing Delta Live Materialized View Table
# MAGIC 1. Spark API equivalent to dlt.read.table() is spark.read.table() and please note spark API dont work for Delta Live Table Views at the time of recording
# MAGIC 1. Use Spark SQL to run complex transformation on source Delta Live Table
# MAGIC 1. Write Spark SQL command directly and run it using SQL as execution language
# MAGIC 1. Prefix LIVE. before Delta Live Table for directly use them in Spark SQL 

# COMMAND ----------

@dlt.table
def dlt_daily_pricing_streaming_trans_1():
        return (
      dlt.read_stream("dlt_daily_pricing_streaming_table")
    )
        

# COMMAND ----------

@dlt.table
def dlt_daily_pricing_streaming_trans_2():
    return (
      spark.sql("""SELECT * FROM LIVE.dlt_daily_pricing_streaming_table""")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 4: Apply Data Quality Rules
# MAGIC
# MAGIC 1. Define Data Quality Rules on Python Dictionary Variable
# MAGIC 1. Use @dlt.expect_all_or_drop(rules) to drop the records not meeting Data Quality Rules
# MAGIC 1. Use .dropDuplicate to remove duplicates based on column(s) values
# MAGIC 1. Redirect bad records into separate table using @dlt.expect_all_or_drop(rules)
# MAGIC

# COMMAND ----------

dqRules = {"Valid_DATETIME_OF_PRICING" : "DATETIME_OF_PRICING IS NOT NULL" }

@dlt.table

@dlt.expect_all_or_drop(dqRules)

def dlt_daily_pricing_streaming_trans_3():
    return (
      dlt.read_stream("dlt_daily_pricing_streaming_table")
      .dropDuplicates(["DATETIME_OF_PRICING", "ROW_ID"])
    )


# COMMAND ----------

badRecordsrules = {}
badRecordsrules["incorrect_record"] = f"NOT({' AND '.join(dqRules.values())})"

@dlt.table
@dlt.expect_all_or_drop(badRecordsrules )
def dlt_daily_pricing_streaming_bad_records():
    return (
      dlt.read_stream("dlt_daily_pricing_streaming_table")
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5: Automate the Change Data Capture(CDC) Using  APPLY CHANGES API 
# MAGIC
# MAGIC 1. Use dlt.create_streaming_table() to Define Targe Table
# MAGIC 1. Read Source Delta Live table using spark.readStream.table()
# MAGIC 1. Use dlt.apply_changes to automatically perform the Change Data Capture between Source and Target Tables
# MAGIC 1. Need to configure dlt.apply_changes with keys and sequence_by columns to identify existing records and select changed records from the source using sequence by column

# COMMAND ----------

@dlt.view
def dlt_daily_pricing_source_view():
    return dlt.read_stream("dlt_daily_pricing_streaming_trans_3")

dlt.create_streaming_table("dlt_daily_pricing_streaming_trans_4")

dlt.apply_changes(
    target = "dlt_daily_pricing_streaming_trans_4",
    source = "dlt_daily_pricing_source_view",
    keys = ["DATETIME_OF_PRICING","ROW_ID"],
    sequence_by = col("source_stream_load_datetime"),
    stored_as_scd_type = "1"
)