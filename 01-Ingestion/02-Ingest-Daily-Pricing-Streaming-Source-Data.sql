-- Databricks notebook source
-- MAGIC %python
-- MAGIC dailyPricingStramingSourceFolderPath = "abfss://bronze@adlsudalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data"

-- COMMAND ----------

USE CATALOG pricing_analytics;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dailyPricingSourceStreamDF = spark.sql("""SELECT
-- MAGIC current_timestamp() as DATETIME_OF_PRICING,
-- MAGIC cast(ROW_ID as bigint) ,
-- MAGIC STATE_NAME,
-- MAGIC MARKET_NAME,
-- MAGIC PRODUCTGROUP_NAME,
-- MAGIC PRODUCT_NAME,
-- MAGIC VARIETY,
-- MAGIC ORIGIN,
-- MAGIC ARRIVAL_IN_TONNES, 
-- MAGIC MINIMUM_PRICE,
-- MAGIC MAXIMUM_PRICE,
-- MAGIC MODAL_PRICE,
-- MAGIC current_timestamp() as source_stream_load_datetime
-- MAGIC FROM bronze.daily_pricing
-- MAGIC WHERE DATE_OF_PRICING='01/01/2023'""")

-- COMMAND ----------

SELECT
    ROW_ID,
    CASE WHEN try_cast(ARRIVAL_IN_TONNES AS decimal(18,2)) IS NULL THEN 'ARRIVAL_IN_TONNES' END AS ARRIVAL_IN_TONNES_ERROR,
    CASE WHEN try_cast(MINIMUM_PRICE AS decimal(36,2)) IS NULL THEN 'MINIMUM_PRICE' END AS MINIMUM_PRICE_ERROR,
    CASE WHEN try_cast(MAXIMUM_PRICE AS decimal(36,2)) IS NULL THEN 'MAXIMUM_PRICE' END AS MAXIMUM_PRICE_ERROR,
    CASE WHEN try_cast(MODAL_PRICE AS decimal(36,2)) IS NULL THEN 'MODAL_PRICE' END AS MODAL_PRICE_ERROR
FROM
    bronze.daily_pricing
WHERE
    DATE_OF_PRICING = '01/01/2023'
    AND (
        try_cast(ARRIVAL_IN_TONNES AS decimal(18,2)) IS NULL OR
        try_cast(MINIMUM_PRICE AS decimal(36,2)) IS NULL OR
        try_cast(MAXIMUM_PRICE AS decimal(36,2)) IS NULL OR
        try_cast(MODAL_PRICE AS decimal(36,2)) IS NULL
    )

-- COMMAND ----------

SELECT * FROM bronze.daily_pricing
WHERE ROW_ID = '14874';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (dailyPricingSourceStreamDF
-- MAGIC .write
-- MAGIC .mode('append')
-- MAGIC .json(dailyPricingStramingSourceFolderPath)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(dailyPricingStramingSourceFolderPath)