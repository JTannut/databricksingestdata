# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName( "pandas to spark").getOrCreate()

# COMMAND ----------

jdbcHostname = "xxxx.database.windows.net"
jdbcDatabase = "x"
jdbcPort = 1433
jdbcAccess = {
"user" : "x",
"password" : "x",
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
"columnEncryptionSetting" : "Enabled",
"keyStoreAuthentication" : "KeyVaultInteractive",
"keyStorePrincipalId" : "xxxxx",
"keyStoreSecret" : "xxxxx" 
}

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase,jdbcAccess)
connectionProperties = {
"user" : "xx",
"password" : "xxxxxx",
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
"columnEncryptionSetting" : "Enabled"
}


# COMMAND ----------

Spdf = spark.read.jdbc(url=jdbcUrl, table="dbo.xxxx", properties=connectionProperties)
display(Spdf)

# COMMAND ----------

# MAGIC %md #SET SECRETS

# COMMAND ----------

# Python code
 
# Get list of all scopes
mysecrets = dbutils.secrets.listScopes()
# Loop through list
for secret in mysecrets:
  print(secret.name)

# COMMAND ----------

dbutils.secrets.get(scope = "xxxxx", key = "xxxxx")


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

spark.conf.set(
2
  "fs.azure.account.key." + dbutils.secrets.get(scope="xxxx",key="xxxxxxx") + ".dfs.core.windows.net",
3
  dbutils.secrets.get(scope="xxxx",key="xxxxx"))

# COMMAND ----------

filePath = "abfss://" + dbutils.secrets.get(scope="xxxxx",key="xxxxx") + "@" + dbutils.secrets.get(scope="xxxxx",key="xxxxxxxxx") + ".dfs.core.windows.net/"

# COMMAND ----------

dbutils.fs.ls(filePath)
