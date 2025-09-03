# Databricks notebook source
# DBTITLE 0,--i18n-67600b38-db2e-44e0-ba4a-1672ee796c77
# MAGIC %md
# MAGIC # Create and Manage Clusters
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/en/compute/index.html">**Databricks Clusters**</a>
# MAGIC
# MAGIC
# MAGIC A Databricks cluster is a set of computation resources and configurations to run the Data Analytical Code that we develop in the Notebook
# MAGIC
# MAGIC There are 5 Type Of the Clusters are available and see below table for their purpose and usage
# MAGIC | Cluster Type | Usage |
# MAGIC | --- | --- |
# MAGIC |All-purpose compute| Running Interactive Workloads and Code Development Purposes|
# MAGIC |Job compute| Running Production Workloads and Automatically Start/Stop by the Code Using|
# MAGIC |SQL Warehouse| Running only SQL Workloads|
# MAGIC |Vector Search| Running Machine Learning Models(LM)|
# MAGIC |Pools| Ready to Use Idle Instances to quickly run the code reducing start time|
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-81a87f46-ce1b-482f-9ad8-62418db25665
# MAGIC %md
# MAGIC ## Create Cluster
# MAGIC
# MAGIC Steps:
# MAGIC 1. Use the left sidebar to navigate to the **Compute** page by clicking on the **Compute** icon
# MAGIC 1. Click the blue **Create Compute** button
# MAGIC 1. For the **Cluster name**, use "Single_Node_Dev_Cluster" 
# MAGIC 1. Set the **Cluster mode** to **Single Node** (This cluster charges minimum amount)
# MAGIC 1. Choose **Databricks runtime version** as one of the LTS(Long Time Support) 14.3 LTS version
# MAGIC 1. Choose *Worker Node Type** as one of the DS3_v2 or DS4_v2 under General Purpose Family of Clusters and Small Size is enough to run Dev Workloads
# MAGIC 1. Leave boxes checked for the default settings under the **Photon Accelaration**
# MAGIC 1. Chnage the Terminate After Settings to 10 Minutes as automatically cluster shutdown if we are not using longer than 10 minutes
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC
# MAGIC It Takes Couple of Minites To Complet

# COMMAND ----------

# DBTITLE 0,--i18n-7323201d-6d28-4780-b2f7-47ab22eadb8f
# MAGIC %md
# MAGIC ## Manage Clusters
# MAGIC
# MAGIC We can view the Cluster under **Compute** page Once the cluster is created
# MAGIC
# MAGIC Click the **Edit** button To change any of the Configurations
# MAGIC
# MAGIC To Start the Cluster Clic **Start** button on the right hand side
# MAGIC
# MAGIC We can **Terminate** and **Delete** the cluster from here