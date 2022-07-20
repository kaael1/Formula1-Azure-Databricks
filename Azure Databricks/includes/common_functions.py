# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#just changing the name to created
from pyspark.sql.functions import current_timestamp
def add_created_date(input_df):
    return input_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    # Ordena as colunas deixando a de partição sempre em último
    for columns_name in input_df.schema.names:
        if columns_name != partition_column:
            column_list.append(columns_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    folder_path = f"abfss://{db_name[3:]}@formula1dlmikael.dfs.core.windows.net/"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
        output_df.write.format("parquet").mode("overwrite").partitionBy(partition_column).save(f"{folder_path}/{table_name}") #Acrescentei para adicionar o arquivo no ADLS uma vez que eu não posso usar Mounts, por conta da subscription    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
        output_df.write.format("parquet").mode("overwrite").partitionBy(partition_column).save(f"{folder_path}/{table_name}") #Acrescentei para adicionar o arquivo no ADLS uma vez que eu não posso usar Mounts, por conta da subscription

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTableSQL = DeltaTable.forName(spark, f"{db_name}.{table_name}")
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTableSQL.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.format("delta").mode("overwrite").partitionBy(partition_column).saveAsTable(f"{db_name}.{table_name}")
    input_df.write.format("delta").mode("overwrite").partitionBy(partition_column).save(f"{folder_path}/{table_name}")

# COMMAND ----------

# db_name = "f1_processed"
# folder_path = f"abfss://{db_name[3:]}@formula1dlmikael.dfs.core.windows.net/"
# final_df = "a"


# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list
