# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

pitstop_schema = StructType([ \
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

df_pitstop = spark.read \
.schema(pitstop_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

final_df = df_pitstop.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
