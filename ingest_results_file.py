# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest Results.JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the Spark DataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read  \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_renamed_df = results_df \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
 \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_ingestion_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted columns

# COMMAND ----------

results_final_df = results_ingestion_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Dedupe the DataFrame

# COMMAND ----------

results_duplicates_removed_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write data to datalake as Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental Load (Parquet) - Method One

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("formula_one_processed.results")):
#         spark.sql(f"ALTER TABLE formula_one_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("formula_one_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental Load (Parquet) - Method Two

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE formula_one_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df, "formula_one_processed", "results", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Merge (Delta) - Preferred Option

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_duplicates_removed_df, "formula_one_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Notebook executed successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM formula_one_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM formula_one_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM formula_one_processed.results
# MAGIC WHERE file_date = "2021-03-21";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM formula_one_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula_one_processed.results
# MAGIC WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------


