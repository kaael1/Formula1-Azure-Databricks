# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC -- LOCATION '/mnt/formula1dl/demo'
# MAGIC 
# MAGIC --DESCRIBE DATABASE f1_demo

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

# %sql
# CREATE TABLE f1_demo.results_external
# USING DELTA
# -- LOCATION '/mnt/formula1dl/demo/results_external'

# COMMAND ----------

results_external_df = spark.read.format("delta").load(f"{demo_folder_path}/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_external")
                                
deltaTable.update("position <= 10", { "points": "21 - position" } )

# COMMAND ----------

df_external = spark.read.format("delta").load(f"{demo_folder_path}/results_external")

# COMMAND ----------

display(df_external)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_external")
                                
deltaTable.delete("points = 0")

# COMMAND ----------

df_external = spark.read.format("delta").load(f"{demo_folder_path}/results_external")
display(df_external)

# COMMAND ----------

# df_external
# f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC            

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)
# MAGIC            

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

drivers_merge_df = spark.sql("SELECT * FROM f1_demo.drivers_merge")

# COMMAND ----------

drivers_merge_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/drivers_merge")

# COMMAND ----------

display(drivers_merge_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

display(spark.read.format("delta").load(f"{demo_folder_path}/drivers_merge"))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 6;

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2022-07-18T23:15:27.000+0000').load("/mnt/formula1dl/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 6;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 6 src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_txn 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_txn 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("parquet").save(f"{demo_folder_path}/drivers_convert_to_delta_new")

# COMMAND ----------

demo_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONVERT TO DELTA parquet.f"{demo_folder_path}/drivers_convert_to_delta_new"

# COMMAND ----------


