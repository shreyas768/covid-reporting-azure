# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "2067d0fe-4753-4d43-82fe-654595317677",
           "fs.azure.account.oauth2.client.secret": "Bhy8Q~idM6I72n9P~4EXnTkWKo8t_Jz6mbBtMaI_",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05ca836c-a7da-4eae-abf0-7b2b2d88b386/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.unmount("/mnt/covidreportingshreyas7dl/raw")

# COMMAND ----------

dbutils.fs.unmount("/mnt/covidreportingshreyas7dl/lookup")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@covidreportingshreyas7dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingshreyas7dl/raw",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@covidreportingshreyas7dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingshreyas7dl/processed",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://lookup@covidreportingshreyas7dl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingshreyas7dl/lookup",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/covidreportingshreyas7dl/raw')

# COMMAND ----------


