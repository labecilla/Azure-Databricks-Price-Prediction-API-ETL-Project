-- Databricks notebook source
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
 LIKE pricing_analytics.gold.reporting_dim_product_gold


-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

UPDATE pricing_analytics.silver.daily_pricing_silver
SET PRODUCTGROUP_NAME='Vegetables',
lakehouse_updated_date = current_timestamp()
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_1 AS
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoadScdType1' AND process_status = 'Completed' );

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_1 
ORDER BY PRODUCT_NAME

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_2 AS 
SELECT 
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,goldDim.PRODUCT_NAME AS GOLD_PRODUCT_NAME
 ,CASE WHEN goldDim.PRODUCT_NAME IS NULL
 THEN ROW_NUMBER() OVER (  ORDER BY silverDim.PRODUCT_NAME,silverDim.PRODUCTGROUP_NAME) 
 ELSE goldDim.PRODUCT_ID END as PRODUCT_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM pricing_analytics.silver.reporting_dim_product_stage_1 silverDim
LEFT OUTER JOIN pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 goldDim
ON silverDim.PRODUCT_NAME= goldDim.PRODUCT_NAME
WHERE goldDim.PRODUCT_NAME IS NULL OR silverDim.PRODUCTGROUP_NAME <> goldDim.PRODUCTGROUP_NAME

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_2

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_3 AS 
SELECT
  silverDim.PRODUCTGROUP_NAME
  ,silverDim.PRODUCT_NAME
,CASE WHEN GOLD_PRODUCT_NAME IS NULL
THEN silverDim.PRODUCT_ID + PREV_MAX_SK_ID 
ELSE PRODUCT_ID END as PRODUCT_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
pricing_analytics.silver.reporting_dim_product_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(PRODUCT_ID),0) as PREV_MAX_SK_ID FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 ) goldDim;

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_3

-- COMMAND ----------

MERGE INTO pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 goldDim
USING pricing_analytics.silver.reporting_dim_product_stage_3 silverDim
ON goldDim.PRODUCT_NAME = silverDim.PRODUCT_NAME
WHEN MATCHED THEN 
UPDATE SET goldDim.PRODUCTGROUP_NAME=silverDim.PRODUCTGROUP_NAME
           ,goldDim.lakehouse_updated_date=current_timestamp()
WHEN NOT MATCHED THEN
INSERT *

-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoadScdType1' , max(lakehouse_updated_date) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver