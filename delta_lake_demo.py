# Databricks notebook source
# MAGIC %md
# MAGIC ### Delta Lake Demo

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to delta lake (managed table)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS formula_one_demo
# MAGIC LOCATION "/mnt/databricksstoragetwo/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databricksstoragetwo/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").saveAsTable("formula_one_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to delta lake (external table)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/databricksstoragetwo/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/databricksstoragetwo/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC Read data from delta lake (Table)

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/databricksstoragetwo/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Read data from delta lake (File)

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").partitionBy("constructorId").saveAsTable("formula_one_demo.results_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW PARTITIONS formula_one_demo.results_partition;

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Lake Tables

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE formula_one_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databricksstoragetwo/demo/results_managed")

deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Delete Delta Lake Tables

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM formula_one_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databricksstoragetwo/demo/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using Merge

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

drivers_day_one_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databricksstoragetwo/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day_one_df.createOrReplaceTempView("drivers_day_one")

# COMMAND ----------

display(drivers_day_one_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day_two_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databricksstoragetwo/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day_two_df.createOrReplaceTempView("drivers_day_two")

# COMMAND ----------

display(drivers_day_two_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day_three_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/databricksstoragetwo/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day_three_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS formula_one_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO formula_one_demo.drivers_merge tgt
# MAGIC USING drivers_day_one upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO formula_one_demo.drivers_merge tgt
# MAGIC USING drivers_day_two upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/databricksstoragetwo/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day_three_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", \
                            "surname" : "upd.surname", "updatedDate" : "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC History & Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge 
# MAGIC VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge
# MAGIC TIMESTAMP AS OF "2022-01-31T10:52:46.000+0000";

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

example_version_df = spark.read.format("delta").option("versionAsOf", "1").load("/mnt/databricksstoragetwo/demo/drivers_merge")

# COMMAND ----------

display(example_version_df)

# COMMAND ----------

example_timestamp_df = spark.read.format("delta").option("timestampAsOf", "2022-01-31T10:52:46.000+0000").load("/mnt/databricksstoragetwo/demo/drivers_merge")

# COMMAND ----------

display(example_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Vacuum (GDPR Data Removal)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge
# MAGIC TIMESTAMP AS OF "2022-01-31T10:52:46.000+0000";

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM formula_one_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM formula_one_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM formula_one_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO formula_one_demo.drivers_merge tgt
# MAGIC USING formula_one_demo.drivers_merge VERSION AS OF 3 src
# MAGIC ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS formula_one_demo.drivers_transaction (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY formula_one_demo.drivers_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO formula_one_demo.drivers_transaction
# MAGIC SELECT * FROM formula_one_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY formula_one_demo.drivers_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO formula_one_demo.drivers_transaction
# MAGIC SELECT * FROM formula_one_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY formula_one_demo.drivers_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM formula_one_demo.drivers_transaction
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY formula_one_demo.drivers_transaction;

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""INSERT INTO formula_one_demo.drivers_transaction
                  SELECT * FROM formula_one_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO formula_one_demo.drivers_transaction
# MAGIC SELECT * FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS formula_one_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO formula_one_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM formula_one_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA formula_one_demo.drivers_convert_to_delta;

# COMMAND ----------

convert_parquet_file_to_delta_df = spark.table("formula_one_demo.drivers_convert_to_delta")

# COMMAND ----------

convert_parquet_file_to_delta_df.write.format("parquet").save("/mnt/databricksstoragetwo/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/databricksstoragetwo/demo/drivers_convert_to_delta_new`

# COMMAND ----------


