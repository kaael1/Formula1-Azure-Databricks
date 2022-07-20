# Databricks notebook source
# MAGIC %md
# MAGIC # Não consegui utilizar mount por causa da limitação da subscription de estudante

# COMMAND ----------

storage_account_name = "formula1dlmikael"
storage_account_key  = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

# COMMAND ----------

def adls(container_name):
    circuits_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

adls("raw")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


