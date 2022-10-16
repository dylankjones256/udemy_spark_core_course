# Databricks notebook source
# MAGIC %md
# MAGIC ### Calculating Race Results with Spark SQL

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS formula_one_presentation.calculated_race_results
(
race_year INT,
team_name STRING,
driver_id INT,
driver_name STRING,
race_id INT,
position INT,
points INT,
calculated_points INT,
created_date TIMESTAMP,
updated_date TIMESTAMP
)
USING DELTA""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW race_results_updated
AS
SELECT races.race_year, 
       constructors.name AS team_name,
       drivers.driver_id,
       drivers.name AS driver_name,
       races.race_id,
       results.position, 
       results.points,
       11 - results.position AS calculated_points
FROM formula_one_processed.results
JOIN formula_one_processed.drivers ON (results.driver_id = drivers.driver_id)
JOIN formula_one_processed.constructors ON (results.constructor_id = constructors.constructor_id)
JOIN formula_one_processed.races ON (results.race_id = races.race_id)
WHERE results.position <= 10
AND results.file_date = '{v_file_date}'""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO formula_one_presentation.calculated_race_results tgt
USING race_results_updated upd
ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
WHEN MATCHED THEN
  UPDATE SET tgt.position = upd.position,
             tgt.points = upd.points,
             tgt.calculated_points = upd.calculated_points,
             tgt.updated_date = current_timestamp
WHEN NOT MATCHED
  THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
  VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM formula_one_presentation.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM formula_one_presentation.calculated_race_results
# MAGIC GROUP BY race_id 
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------


