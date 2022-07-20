-- Databricks notebook source
-- MAGIC %md
-- MAGIC # OBS: ESSA PARTE DO PROJETO NÃO É MAIS UTILIZADA

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table
-- MAGIC 
-- MAGIC obs: precisei criar as tabelas usando saveAsTable pois minha inscrição no azure é de estudante, então eu não tinha acesos ao MNT

-- COMMAND ----------


 -- exemplo, não posso utilizar assim por conta da subscription
 -- %python
 /*
# CREATE TABLE IF NOT EXISTS f1_raw.circuits(
#   circuitRef STRING,
#   name STRING,
#   location STRING,
#   country, STRING,
#   lat DOUBLE,
#   lng DOUBLE,
#   alt INT,
#   url STRING
# )
# USING csv
# OPTIONS (path "/mnt/formuladl/raw/circuits.csv") */


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

-- COMMAND ----------

--Circuits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
-- MAGIC                                      StructField("circuitRef", StringType(), True),
-- MAGIC                                      StructField("name", StringType(), True),
-- MAGIC                                      StructField("location", StringType(), True),
-- MAGIC                                      StructField("country", StringType(), True),
-- MAGIC                                      StructField("lat", DoubleType(), True),
-- MAGIC                                      StructField("lng", DoubleType(), True),
-- MAGIC                                      StructField("alt", IntegerType(), True),
-- MAGIC                                      StructField("url", StringType(), True),
-- MAGIC     
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).csv(f"{raw_folder_path}/circuits.csv", header="true")

-- COMMAND ----------

drop table if exists f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.circuits")

-- COMMAND ----------

--Races

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType(fields=[
-- MAGIC     StructField("raceId", IntegerType(), False),
-- MAGIC     StructField("year", IntegerType(), True),
-- MAGIC     StructField("round", IntegerType(), True),
-- MAGIC     StructField("circuitId", IntegerType(), True),
-- MAGIC     StructField("name", StringType(), True),
-- MAGIC     StructField("date", DateType(), True),
-- MAGIC     StructField("time", StringType(), True),
-- MAGIC     StructField("url", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).csv(f"{raw_folder_path}/races.csv", header="true")

-- COMMAND ----------

drop table if exists f1_raw.races;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.races")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Agora com JSON

-- COMMAND ----------

--constructores

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %md
-- MAGIC /*
-- MAGIC DROP TABLE IF EXISTS f1_raw.constructors
-- MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
-- MAGIC   constructorId INT,
-- MAGIC   constructorRef STRING,
-- MAGIC   name STRING,
-- MAGIC   nationality STRING,
-- MAGIC   url STRING
-- MAGIC )
-- MAGIC USING json
-- MAGIC OPTIONS(path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).json(f"{raw_folder_path}/constructors.json")

-- COMMAND ----------

drop table if exists f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.constructors")

-- COMMAND ----------

--drivers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %md
-- MAGIC /*
-- MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
-- MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
-- MAGIC   driverId INT,
-- MAGIC   driverRef STRING,
-- MAGIC   number INT,
-- MAGIC   code STRING,
-- MAGIC   name STRUCT<forename: STRING, surname: STRING>,
-- MAGIC   dob DATE,
-- MAGIC   nationality STRING,
-- MAGIC   url STRING)
-- MAGIC USING json
-- MAGIC OPTIONS(path "/mnt/formula1dl/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, concat, current_timestamp, lit

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC name_schema = StructType([ \
-- MAGIC     StructField("forename", StringType(), True),
-- MAGIC     StructField("surname", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #opcao 1
-- MAGIC name_schema = StructType([ \
-- MAGIC     StructField("forename", StringType(), True),
-- MAGIC     StructField("surname", StringType(), True),
-- MAGIC 
-- MAGIC ])
-- MAGIC 
-- MAGIC df_schema = StructType([ \
-- MAGIC     StructField("driverId", IntegerType(), False),
-- MAGIC     StructField("driverRef", StringType(), True),
-- MAGIC     StructField("number", IntegerType(), True),
-- MAGIC     StructField("code", StringType(), True),
-- MAGIC     StructField("name", name_schema),
-- MAGIC     StructField("dob", DateType(), True),
-- MAGIC     StructField("nationality", StringType(), True),
-- MAGIC     StructField("url", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).json(f"{raw_folder_path}/drivers.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.drivers")

-- COMMAND ----------

--RESULTS

-- COMMAND ----------

--mesma coisa que o drivers porém com outra tipagem


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType([
-- MAGIC     StructField("resultId", IntegerType(), False),
-- MAGIC     StructField("raceId", IntegerType(), False),
-- MAGIC     StructField("driverId", IntegerType(), False),
-- MAGIC     StructField("constructorId", IntegerType(), False),
-- MAGIC     StructField("number", IntegerType(), True),
-- MAGIC     StructField("grid", IntegerType(), False),
-- MAGIC     StructField("position", IntegerType(), True),
-- MAGIC     StructField("positionText", StringType(), False),
-- MAGIC     StructField("positionOrder", IntegerType(), False),
-- MAGIC     StructField("points", DoubleType(), False),
-- MAGIC     StructField("laps", IntegerType(), False),
-- MAGIC     StructField("time", StringType(), True),
-- MAGIC     StructField("milliseconds", IntegerType(), True),
-- MAGIC     StructField("fastestlap", IntegerType(), True),
-- MAGIC     StructField("rank", IntegerType(), True),
-- MAGIC     StructField("fastestLapTime", StringType(), True),
-- MAGIC     StructField("statusId", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).json(f"{raw_folder_path}/results.json")

-- COMMAND ----------

drop table if exists f1_raw.results;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.results")

-- COMMAND ----------

--pistop

-- COMMAND ----------

--
--mesma coisa porém como é multilne json:
--OPTIONS(path "/mnt/formula1dl/raw/constructors.json", multiLine true)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType([ \
-- MAGIC     StructField("raceId", IntegerType(), False),
-- MAGIC     StructField("driverId", IntegerType(), True),
-- MAGIC     StructField("stop", StringType(), True),
-- MAGIC     StructField("lap", IntegerType(), True),
-- MAGIC     StructField("time", StringType(), True),
-- MAGIC     StructField("duration", StringType(), True),
-- MAGIC     StructField("milliseconds", IntegerType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).option("multiline", True).json(f"{raw_folder_path}/pit_stops.json")

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.pit_stops")

-- COMMAND ----------

-- lap times

-- COMMAND ----------

-- Multiple files
-- CSV FILES

-- OPTIONS(path "/mnt/formula1dl/raw/lap_times")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType([ \
-- MAGIC     StructField("raceId", IntegerType(), False),
-- MAGIC     StructField("driverId", IntegerType(), True),
-- MAGIC     StructField("stop", StringType(), True),
-- MAGIC     StructField("lap", IntegerType(), True),
-- MAGIC     StructField("time", StringType(), True),
-- MAGIC     StructField("duration", StringType(), True),
-- MAGIC     StructField("milliseconds", IntegerType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).csv(f"{raw_folder_path}/lap_times")

-- COMMAND ----------

drop table if exists f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.lap_times")

-- COMMAND ----------

select count(1) from f1_raw.lap_times;

-- COMMAND ----------

-- qualifying

-- json file
-- multiline json
-- multiple files

-- COMMAND ----------

-- OPTIONS(path "/mnt/formula1dl/raw/lap_times", multiLine true)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema = StructType([ \
-- MAGIC     StructField("qualifyId", IntegerType(), False),
-- MAGIC     StructField("raceId", IntegerType(), True),
-- MAGIC     StructField("driverId", IntegerType(), True),
-- MAGIC     StructField("constructorId", IntegerType(), True),
-- MAGIC     StructField("number", IntegerType(), True),
-- MAGIC     StructField("position", IntegerType(), True),
-- MAGIC     StructField("q1", StringType(), True),
-- MAGIC     StructField("q2", StringType(), True),
-- MAGIC     StructField("q3", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.schema(df_schema).option("multiLine", True).json(f"{raw_folder_path}/qualifying")

-- COMMAND ----------

drop table if exists f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_raw.qualifying")
