-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Segoe UI">Report on Dominant Formula One Teams <h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

USE formula_one_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM formula_one_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM formula_one_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM formula_one_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

SELECT race_year,
       team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS average_points
FROM formula_one_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------


