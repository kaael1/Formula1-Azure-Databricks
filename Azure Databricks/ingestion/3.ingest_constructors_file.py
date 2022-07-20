# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest constructores.json file

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

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

#outro jeito de se criar um schema pro dataframe
constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

#constructor_dropped_df = constructor_df.drop("url")
constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#a linha abaixo cria tanto a tabela no SQL do databricks quanto no datalake, porém como não estou usando mounts por causa da azure student subscription eu não consegui criar o processed database com o LOCATION correto, então estou criando o arquivo no azure datalake e no DSQL separado

#xxxx_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.xxxx")

constructor_final_df.write.mode("overwrite").saveAsTable("f1_processed.constructors")
constructor_final_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
