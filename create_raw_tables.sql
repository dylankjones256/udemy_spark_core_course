-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creating raw tables for CSV, JSON and Multiple Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula_one_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.circuits;
CREATE TABLE IF NOT EXISTS formula_one_raw.circuits
(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/databricksstoragetwo/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM formula_one_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.races;
CREATE TABLE IF NOT EXISTS formula_one_raw.races
(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS(path "/mnt/databricksstoragetwo/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM formula_one_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Constructors Table
-- MAGIC 
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.constructors;
CREATE TABLE IF NOT EXISTS formula_one_raw.constructors
(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/databricksstoragetwo/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM formula_one_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drivers Table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.drivers;
CREATE TABLE IF NOT EXISTS formula_one_raw.drivers
(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/databricksstoragetwo/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM formula_one_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Results Table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.results;
CREATE TABLE IF NOT EXISTS formula_one_raw.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
USING json
OPTIONS(path "/mnt/databricksstoragetwo/raw/results.json")

-- COMMAND ----------

SELECT * FROM formula_one_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pit Stops Table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.pit_stops;
CREATE TABLE IF NOT EXISTS formula_one_raw.pit_stops
(
raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS(path "/mnt/databricksstoragetwo/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM formula_one_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.lap_times;
CREATE TABLE IF NOT EXISTS formula_one_raw.lap_times
(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS(path "/mnt/databricksstoragetwo/raw/lap_times")

-- COMMAND ----------

SELECT * FROM formula_one_raw.lap_times

-- COMMAND ----------

SELECT COUNT(1) FROM formula_one_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS formula_one_raw.qualifying;
CREATE TABLE IF NOT EXISTS formula_one_raw.qualifying
(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS(path "/mnt/databricksstoragetwo/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM formula_one_raw.qualifying

-- COMMAND ----------

DESC EXTENDED formula_one_raw.qualifying

-- COMMAND ----------


