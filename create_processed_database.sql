-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create Processed Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula_one_processed
LOCATION "/mnt/databricksstoragetwo/processed"

-- COMMAND ----------

DESC DATABASE formula_one_processed

-- COMMAND ----------


