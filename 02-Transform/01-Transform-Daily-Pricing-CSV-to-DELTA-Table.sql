-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Notebook Name : 01-Transform-Daily-Pricing-CSV-to-DELTA-Table
-- MAGIC ##### Source Table Details
-- MAGIC Source Table Name : pricing_analytics.bronze.daily_pricing
-- MAGIC Source Table New/Changed-Records Identification Column : source_file_load_date
-- MAGIC
-- MAGIC ##### Target Table Details
-- MAGIC Target Table Name : pricing_analytics.silver.daily_pricing_silver
-- MAGIC
-- MAGIC
-- MAGIC ##### Processrunlogs Table For Inctemental Load
-- MAGIC Processrunlogs Table Name : pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
-- MAGIC

-- COMMAND ----------

USE CATALOG pricing_analytics;

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.daily_pricing_silver;

CREATE TABLE IF NOT EXISTS silver.daily_pricing_silver(
  DATE_OF_PRICING DATE,
  ROW_ID BIGINT,
  STATE_NAME STRING,
  MARKET_NAME STRING,
  PRODUCTGROUP_NAME STRING,
  PRODUCT_NAME STRING,
  VARIETY STRING,
  ORIGIN STRING,
  ARRIVAL_IN_TONNES DECIMAL(18,2),
  MINIMUM_PRICE DECIMAL(36,2),
  MAXIMUM_PRICE DECIMAL(36,2),
  MODAL_PRICE DECIMAL(36,2),
  source_file_load_date TIMESTAMP,
  lakehouse_inserted_date TIMESTAMP,
  lakehouse_updated_date TIMESTAMP
)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

SELECT COUNT(*) AS total_rows, 
       COUNT(source_file_load_date) AS non_null_source_file_load_date
FROM pricing_analytics.silver.daily_pricing_silver;

-- COMMAND ----------

SELECT DISTINCT source_file_load_date FROM pricing_analytics.silver.daily_pricing_silver;

SELECT * FROM pricing_analytics.silver.daily_pricing_silver;

SELECT * FROM pricing_analytics.bronze.daily_pricing;

SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') 
FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'daily_pricing_silver' AND process_status = 'Completed';


-- COMMAND ----------



-- COMMAND ----------

SELECT DISTINCT source_file_load_date FROM pricing_analytics.bronze.daily_pricing;

-- COMMAND ----------

USE CATALOG pricing_analytics;
INSERT INTO  silver.daily_pricing_silver
SELECT
  to_date(DATE_OF_PRICING,'dd/MM/yyyy'),
  cast(ROW_ID as bigint) ,
  STATE_NAME,
  MARKET_NAME,
  PRODUCTGROUP_NAME,
  PRODUCT_NAME,
  VARIETY,
  ORIGIN,
  ARRIVAL_IN_TONNES,
  MINIMUM_PRICE,
  MAXIMUM_PRICE,
  MODAL_PRICE,
  source_file_load_date ,
  current_timestamp(),
  current_timestamp()
FROM pricing_analytics.bronze.daily_pricing
WHERE source_file_load_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'daily_pricing_silver' AND process_status = 'Completed' )


-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'daily_pricing_silver' , max(cast(source_file_load_date AS timestamp)),'Completed' FROM pricing_analytics.silver.daily_pricing_silver;

-- COMMAND ----------

SELECT 'daily_pricing_silver' , max(cast(source_file_load_date AS timestamp)) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver;

-- COMMAND ----------

SELECT DISTINCT source_file_load_date FROM pricing_analytics.silver.daily_pricing_silver;

-- COMMAND ----------

--ALTER TABLE pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS
--ADD COLUMN (PROCESSED_TABLE_DATETIME TIMESTAMP);

SELECT * FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS;