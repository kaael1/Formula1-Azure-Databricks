# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

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

qualifying_schema = StructType([ \
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

#lap_times/lap_times_split*.csv para pegar todos os arquivos, like...
df_qualifying = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

final_df = df_qualifying.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorId", "constructorId") \
                                   .withColumnRenamed("qualifyId", "qualifying_id") \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')
merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.constructorId = src.constructorId"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC   from f1_processed.qualifying
# MAGIC   group by race_id
# MAGIC   order by race_id DESC

# COMMAND ----------


