-- Databricks notebook source
USE formula_one_processed;

-- COMMAND ----------

SELECT *, 
CONCAT(driver_ref, "-", code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, " ")[0] forename, SPLIT(name, " ")[1] surname
FROM drivers

-- COMMAND ----------

SELECT *, current_timestamp()
FROM drivers

-- COMMAND ----------

SELECT *, date_format(dob, "dd-MM-yyyy")
FROM drivers

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers

-- COMMAND ----------

SELECT MIN(dob)
FROM drivers

-- COMMAND ----------

SELECT * 
FROM drivers
WHERE dob = "1896-12-28"

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers
WHERE nationality = "Thai"

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality

-- COMMAND ----------

SELECT name, nationality, dob
FROM drivers
WHERE (nationality = "East German"
AND dob <= "1989-01-01")
OR (nationality = "German"
AND dob >= "1989-01-01")
ORDER BY dob DESC;

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 50
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank

-- COMMAND ----------


