# Databricks notebook source
# MAGIC %md
# MAGIC ### Driver Standings Transformation from F1 Driver Standings Page

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Find race_year values for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = column_to_list(race_results_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Aggregation by sum of points

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points"), count(when(col("position") == 1, True))) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(CASE WHEN (position = 1) THEN true END)", "wins")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Creating a window function for the rank column

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write data to datalake as Delta

# COMMAND ----------

# overwrite_partition(final_df, "formula_one_presentation", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "formula_one_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM formula_one_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM formula_one_presentation.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_presentation.driver_standings
# MAGIC WHERE race_year = 2021;

# COMMAND ----------


