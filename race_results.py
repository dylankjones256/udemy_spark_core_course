# Databricks notebook source
# MAGIC %md
# MAGIC ### Race Results Transformation from F1 Results Page
# MAGIC 
# MAGIC (https://www.bbc.co.uk/sport/formula1/latest)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Instantiate DataFrames with processed data from required tables

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Join the 'circuits_df' to the 'races_df', selecting required columns

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Join drivers_df and constructors_df to race_circuits_df, and then join that to results_df

# COMMAND ----------

race_results_df = results_df \
.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", \
                                 "driver_nationality", "team", "grid", "position", "fastest_lap", "race_time", "points", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as Delta

# COMMAND ----------

# overwrite_partition(final_df, "formula_one_presentation", "race_results", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "formula_one_presentation", "race_results", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_presentation.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM formula_one_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


