# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()

# COMMAND ----------


