# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting DBFS to Azure Data Lake Storage

# COMMAND ----------

dbutils.secrets.listScopes()
print("\n")
dbutils.secrets.list("formula_one_scope")
print("\n")
dbutils.secrets.get(scope ="formula_one_scope", key = "databricks-client-id")

# COMMAND ----------

# MAGIC %md 
# MAGIC First, let's start by getting all the variables from Azure for authentication
# MAGIC 
# MAGIC (Originally, this held the hardcoded values but we created a Key Vault with a Secret Scope for our credentials to be safely stored in)

# COMMAND ----------

storage_account_name = "databricksstoragetwo"
client_id            = dbutils.secrets.get(scope ="formula_one_scope", key = "databricks-client-id")
tenant_id            = dbutils.secrets.get(scope ="formula_one_scope", key = "databricks-tenant-id")
client_secret        = dbutils.secrets.get(scope ="formula_one_scope", key = "databricks-client-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can write out our configuration for this notebook

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Normally, your mounting process is handled by a function - let's make one for our containers below
# MAGIC 
# MAGIC We need to pass our container_name variable so the function can be called with container names

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

# MAGIC %md
# MAGIC Just to be sure, let's check that our containers have now been mounted

# COMMAND ----------

dbutils.fs.ls("/mnt/databricksstoragetwo/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/databricksstoragetwo/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/databricksstoragetwo/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/databricksstoragetwo/demo")

# COMMAND ----------

# MAGIC %md
# MAGIC To summarise, we’ve replaced our hardcoded secrets with the dbutils.secrets.get() commands,
# MAGIC 
# MAGIC Our configs have been set to retrieve values from this those commands, 
# MAGIC 
# MAGIC and our mount_adls function takes the container name, mounting the storage and containers for us!
