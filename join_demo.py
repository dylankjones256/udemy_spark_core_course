# Databricks notebook source
# MAGIC %md
# MAGIC ### A demo to try the join commands in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Import our configuration, and build a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC As we're doing a join, we'll need to create two DataFrames!

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we're importing the DataFrames for our join demo, changing the 'name' column to be less ambiguous

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Inner Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Outer Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Semi Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Anti Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The above joins may look confusing, but you can see that the Semi Join returns everything but the 'right' DataFrame columns, and the Anti Join does the opposite - returning everything that isn't returned in the Semi Join - this makes sense, because the total number of rows between the two joins is still 69, which fits our earlier query!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Cross Join

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Above, you can see that Crossjoins multiply the number of rows in the first table with the number of rows in the second table
# MAGIC 
# MAGIC As an example, 1000 rows * 1000 rows would make a table with 1,000,000 rows - not ideal!
# MAGIC 
# MAGIC !["Cartesian Product"](https://upload.wikimedia.org/wikipedia/commons/thumb/4/4e/Cartesian_Product_qtl1.svg/330px-Cartesian_Product_qtl1.svg.png)

# COMMAND ----------


