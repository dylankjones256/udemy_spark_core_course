# Databricks notebook source
# MAGIC %md
# MAGIC ### Test for Global Temp View

# COMMAND ----------

# MAGIC %md
# MAGIC This view throws an exception, as it is local to the sq_temp_view_demo notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM v_race_results

# COMMAND ----------

# MAGIC %md
# MAGIC This view does not throw an exception, because it is a global temp view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------


