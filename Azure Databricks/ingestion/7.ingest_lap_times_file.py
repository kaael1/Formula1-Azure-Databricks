# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType

# COMMAND ----------

lap_times_schema = StructType([ \
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("miliseconds", IntegerType(), True),
])

# COMMAND ----------

#lap_times/lap_times_split*.csv para pegar todos os arquivos, like...
df_lap_times = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

final_df = df_lap_times.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_processed.lap_times

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC   from f1_processed.lap_times
# MAGIC   group by race_id
# MAGIC   order by race_id DESC

# COMMAND ----------


