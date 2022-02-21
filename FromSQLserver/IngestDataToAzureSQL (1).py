# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC val url = "jdbc:sqlserver://hrthadvdardbs01.database.windows.net:1433;database=xxxxxx;user=xxxxxxxxx;password=xxxxxxxxxxx"

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC val myproperties = new Properties()
# MAGIC myproperties.put("user", "xxxxx")
# MAGIC myproperties.put("password", "xxxxxxxxxxx")

# COMMAND ----------

# MAGIC %scala
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC myproperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC val mydf = spark.read.format("csv")
# MAGIC     .option("header","true")
# MAGIC     .option("inferSchema", "true")
# MAGIC     .option("delimiter", "|") 
# MAGIC           .load("/FileStore/tables/productsale1.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(mydf)

# COMMAND ----------

# MAGIC %md ## LOAD DATA TO AZURE SQL WITH python 

# COMMAND ----------

jdbcHostname = "xxxxxxx.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "xxxxxxxx"
properties = {
 "user" : "xxxxxxx",
 "password" : "xxxxxxxxxxxxx" }

# COMMAND ----------

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
mydf = sqlContext.read.csv("/FileStore/tables/productsale1.csv",header=True)


# COMMAND ----------

from pyspark.sql import *
import pandas as pd
myfinaldf = DataFrameWriter(mydf)
myfinaldf.jdbc(url=url, table= "productFromdatabricks", mode ="overwrite", properties = properties)

# COMMAND ----------

# MAGIC %md # LOAD DATA FROM AZURE SQL

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName( "pandas to spark").getOrCreate()

# COMMAND ----------

jdbcHostname = "xxxxxxxxxxxx.database.windows.net"
jdbcDatabase = "xxxxxxx"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
"user" : "xxxxxxxx",
"password" : "xxxxxxxxxxx",
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

Spdf = spark.read.jdbc(url=jdbcUrl, table="datatier2", properties=connectionProperties)
display(Spdf)

# COMMAND ----------

# Python code
 
# Get list of all scopes
mysecrets = dbutils.secrets.listScopes()
# Loop through list
for secret in mysecrets:
  print(secret.name)

# COMMAND ----------

dbutils.secrets.get(scope = "forconnecttoKeyvault", key = "txnnxt")


# COMMAND ----------

Spdf = spark.read.jdbc(url=jdbcUrl, table="datatier2", properties=connectionProperties)
display(Spdf)

# COMMAND ----------

 
dbutils.fs.mount(
  source = "xxxxxxxxxxxxxxxxxxx.database.windows.net",
  mount_point = "/mnt/testdata1",
  extra_configs = {"fs.azure.account.key.bitools2.blob.core.windows.net":dbutils.secrets.get(scope = "forconnecttoKeyvault", key = "txnnxt")})

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
url = "jdbc:sql:RTK=5246...;User=xxxxxxxx;Password=xxxxxxxx;Database=xxxxxxxx;Server=xxxxxxxxxxxxxxxx;Port=1433;"


# COMMAND ----------

remote_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .load()

# COMMAND ----------

pip install adal

# COMMAND ----------

databricks secrets put --scope <scope-name> --key <key-name>

# COMMAND ----------

import adal

resource_app_id_url = "https://database.windows.net/"

service_principal_id = dbutils.secrets.get(scope = "defaultScope", key = "DatabricksSpnId")
service_principal_secret = dbutils.secrets.get(scope = "defaultScope", key = "DatabricksSpnSecret")
tenant_id = dbutils.secrets.get(scope = "defaultScope", key = "TenantId")
authority = "https://login.windows.net/" + tenant_id


azure_sql_url = "jdbc:sqlserver://xxxxxxxxxxxxxxxxx.database.windows.net"
database_name = "xxxxxxx"
db_table = "dbo.xxxxxxxxx" 


encrypt = "true"
host_name_in_certificate = "*.database.windows.net"

context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]

emperorDf = spark.read \
             .format("com.microsoft.sqlserver.jdbc.spark") \
             .option("url", azure_sql_url) \
             .option("dbtable", db_table) \
             .option("databaseName", database_name) \
             .option("accessToken", access_token) \
             .option("encrypt", "true") \
             .option("hostNameInCertificate", "*.database.windows.net") \
             .load()
             
display(emperorDf)    

# COMMAND ----------


