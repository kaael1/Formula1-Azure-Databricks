# Databricks notebook source
storage_account_name = "formula1dlmikael"
storage_account_key  = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

# COMMAND ----------

raw_folder_path = f"abfss://raw@formula1dlmikael.dfs.core.windows.net/"
processed_folder_path = f"abfss://processed@formula1dlmikael.dfs.core.windows.net/"
presentation_folder_path = f"abfss://presentation@formula1dlmikael.dfs.core.windows.net/"
demo_folder_path = f"abfss://demo@formula1dlmikael.dfs.core.windows.net/"

# COMMAND ----------


