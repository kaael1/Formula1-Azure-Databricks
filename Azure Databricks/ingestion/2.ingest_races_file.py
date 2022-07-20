# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

#add ingestion date column
races_added_df = races_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_selected_df = races_added_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"), col("file_date"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

#a linha abaixo cria tanto a tabela no SQL do databricks quanto no datalake, porém como não estou usando mounts por causa da azure student subscription eu não consegui criar o processed database com o LOCATION correto, então estou criando o arquivo no azure datalake e no DSQL separado

#xxxx_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.xxxx")

races_selected_df.write.mode("overwrite").saveAsTable("f1_processed.races")
races_selected_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
