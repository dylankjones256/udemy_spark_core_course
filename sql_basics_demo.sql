-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE formula_one_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT *
FROM formula_one_processed.drivers;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

DESC EXTENDED drivers;

-- COMMAND ----------

SELECT * FROM formula_one_processed.drivers

-- COMMAND ----------

SELECT name, dob
FROM drivers
WHERE nationality = "British"
AND
dob >= "1990-01-01"
ORDER BY dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob
FROM drivers
WHERE (nationality = "German"
AND dob <= "1989-01-1") 
OR (nationality = "Russian"
AND dob >= "1991-01-01")
ORDER BY dob DESC;

-- COMMAND ----------


