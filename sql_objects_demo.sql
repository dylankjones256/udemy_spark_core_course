-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Python

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC TABLE EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create External table using Python
-- MAGIC 1. Create External table using SQL
-- MAGIC 1. Effect of dropping a External table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_ext_python")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_python;

-- COMMAND ----------

SELECT * FROM race_results_ext_python
WHERE race_year = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL

-- COMMAND ----------

USE demo

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

--Instructor's method for creating External table with SQL
--Specifying a location is how you create External rather than Managed tables

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
position INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/databricksstoragetwo/presentation/race_results_ext_sql"

-- COMMAND ----------

-- This cell threw an AnalysisException - it cannot cast the 'points' column from string to float
-- However, if I use CREATE TABLE table_name AS data etc, the table will be populated with the correct data
-- The below command will work then, but it is duplicating the data we already added
-- SOLVED - The schema had a column in the wrong order!

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2012

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2000

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------


