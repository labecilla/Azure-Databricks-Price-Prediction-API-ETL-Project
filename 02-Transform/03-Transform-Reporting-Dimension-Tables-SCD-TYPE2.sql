-- Databricks notebook source
SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

SHOW CREATE TABLE pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 (
  PRODUCTGROUP_NAME STRING,
  PRODUCT_NAME STRING,
  PRODUCT_ID BIGINT,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  lakehouse_inserted_date TIMESTAMP,
  lakehouse_updated_date TIMESTAMP)
USING delta

-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 WHERe PRODUCT_NAME='Onion'

-- COMMAND ----------

UPDATE pricing_analytics.silver.daily_pricing_silver
SET PRODUCTGROUP_NAME='Oil Seeds ',
lakehouse_updated_date = current_timestamp()
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_1 AS
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoadScdType2' AND process_status = 'Completed' );

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_2 AS 
SELECT 
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,goldDim.PRODUCT_NAME AS GOLD_PRODUCT_NAME
  ,goldDim.PRODUCT_ID AS GOLD_PRODUCT_ID
,ROW_NUMBER() OVER (  ORDER BY silverDim.PRODUCT_NAME,silverDim.PRODUCTGROUP_NAME)  as PRODUCT_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM pricing_analytics.silver.reporting_dim_product_stage_1 silverDim
LEFT OUTER JOIN pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 goldDim
ON silverDim.PRODUCT_NAME= goldDim.PRODUCT_NAME
WHERE goldDim.PRODUCT_NAME IS NULL OR silverDim.PRODUCTGROUP_NAME <> goldDim.PRODUCTGROUP_NAME

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_3 AS 
SELECT
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,GOLD_PRODUCT_ID
 ,silverDim.PRODUCT_ID + PREV_MAX_SK_ID  as PRODUCT_ID
,CASE WHEN GOLD_PRODUCT_NAME IS NULL THEN 'New' Else 'Changed' End as RECORD_STATUS
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
pricing_analytics.silver.reporting_dim_product_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(PRODUCT_ID),0) as PREV_MAX_SK_ID FROM  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 ) goldDim;

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_3 
--WHERE RECORD_STATUS='Changed'

-- COMMAND ----------

MERGE INTO  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 goldDim
USING pricing_analytics.silver.reporting_dim_product_stage_3 silverDim
ON goldDim.PRODUCT_ID = silverDim.GOLD_PRODUCT_ID
WHEN MATCHED THEN 
UPDATE SET goldDim.end_date=current_timestamp()
          ,goldDim.lakehouse_updated_date=current_timestamp()
WHEN NOT MATCHED  THEN
INSERT (PRODUCTGROUP_NAME,PRODUCT_NAME,PRODUCT_ID,start_date,end_date,lakehouse_inserted_date,lakehouse_updated_date)
VALUES (silverDim.PRODUCTGROUP_NAME,silverDim.PRODUCT_NAME,silverDim.PRODUCT_ID,current_timestamp(),NULL,current_timestamp(),current_timestamp())


-- COMMAND ----------

INSERT INTO pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 
SELECT
PRODUCTGROUP_NAME
,PRODUCT_NAME
,PRODUCT_ID
,current_timestamp()
,NULL
,current_timestamp()
,current_timestamp()
FROM pricing_analytics.silver.reporting_dim_product_stage_3
WHERE RECORD_STATUS ='Changed'

-- COMMAND ----------

SELECT * FROM  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoadScdType2' , max(lakehouse_updated_date) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver