# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

df_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", DoubleType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestlap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read.schema(df_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_with_columns_df = results_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumnRenamed("resultId", "result_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorId", "constructor_id") \
                                   .withColumnRenamed("positionText", "position_text") \
                                   .withColumnRenamed("positionOrder", "position_order") \
                                   .withColumnRenamed("fastestLap", "fastest_lap") \
                                   .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                   .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_processed", "results", "race_id")
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


