# Databricks notebook source
# MAGIC %md
# MAGIC ### Access DataFrame using SQL
# MAGIC 
# MAGIC ### Objectives
# MAGIC 
# MAGIC 1. Create Temporary Views on DataFrames
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * from v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Example of spark.sql parameter feature

# COMMAND ----------

p_race_year = 2000

# COMMAND ----------

race_results_example_df = spark.sql(f"SELECT * from v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary Views
# MAGIC 
# MAGIC ### Objectives
# MAGIC 
# MAGIC 1. Create global temporary views on DataFrames
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

spark.sql("SELECT * \
  FROM global_temp.gv_race_results").show()

# COMMAND ----------


