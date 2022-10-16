# Databricks notebook source
# MAGIC %md
# MAGIC ### A demo to try the filter commands in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Import our configuration, and build a DataFrame

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Filter Transformation, in an SQL and a Pythonic way

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

races_df_filter_sql = races_df.filter("race_year = 2019")

# COMMAND ----------

display(races_df_filter_sql)

# COMMAND ----------

races_df_filter_sql = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

display(races_df_filter_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

races_df_filter_python = races_df.filter(races_df.race_year == 2019)

# COMMAND ----------

display(races_df_filter_python)

# COMMAND ----------

# We have to enclose our two conditions in their own brackets because they are separate actions!
races_df_filter_python = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_df_filter_python)
