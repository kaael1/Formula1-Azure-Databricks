# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([ \
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
])

# COMMAND ----------

#opcao 1
name_schema = StructType([ \
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),

])

drivers_schema = StructType([ \
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

#a linha abaixo cria tanto a tabela no SQL do databricks quanto no datalake, porém como não estou usando mounts por causa da azure student subscription eu não consegui criar o processed database com o LOCATION correto, então estou criando o arquivo no azure datalake e no DSQL separado

#xxxx_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.xxxx")

drivers_final_df.write.mode("overwrite").saveAsTable("f1_processed.drivers")
drivers_final_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
