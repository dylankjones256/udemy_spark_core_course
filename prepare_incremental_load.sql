-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Drop all tables and databases 

-- COMMAND ----------

DROP DATABASE IF EXISTS formula_one_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula_one_processed
LOCATION "/mnt/databricksstoragetwo/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS formula_one_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula_one_presentation
LOCATION "/mnt/databricksstoragetwo/presentation";
