# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC 
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC 
# MAGIC apt-get update
# MAGIC 
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC 
# MAGIC apt-get -y install unixodbc-dev
# MAGIC 
# MAGIC sudo apt-get install python3-pip -y
# MAGIC 
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

def upsertAzureSQL(df, azureSqlStagingTable, azureSqlTargetTable, lookupColumns, deltaName):
    
#source
targetTableTest = "Target"
stagingTableTest = "Source"

#readColumnFromDataFrame

dfColumns = str(df.columns)
dfColumns= (((dfColumns.replace("'","")).replace("[","")).replace("]","")).replace(" ",'')


#Create a MERGE SQL for SCD UPSERT
mergeStatement = "MERGE " + azureSqlTargetTable + " as " + targetTableTest + " Using " + azureSqlStagingTable \ + " as " + stagingTableTest + " ON ("


#Lookup column Statement 

if(lookupColumns is not None or lookupColumns is len(lookColumns) > 0):
    uniqueCols = lookupColumns.split("|")
    lookupStatement =""
    for lookupCol in uniqueCols:
        lookupStatement = lookupStatement + targetTableTest + "." + lookupCol + " = " + stagingTableTest + "." + lookupCol + " and "
        #print("insertSQL={}".format(insertSQL))
        
#Update MERGE Statement ( deltaColumn )

if deltaName is not None and len(deltaName) >0:
    #check if the last update is greater than existing record
    updateStatement = lookupStatement + stagingTableTest + "." + deltaName + " >= " + targetTableTest + "." + deltaName
else
    #remove list and
    remove = "and"
    reverse_remove=remove[::-1]
    updateStatement = lookupStatement[::-1].replace(reverse_remove,"",1)[::-1]
    

    
if deltaName is not None and len(deltaName) > 0:
    #check if last update is lesser then existing record
    updateStatement = updateStatement + " and " + targetTableTest + "." + deltaName + " < " + stagingTableTest + "." + deltaName
    
    # add when matched

updateStatement = updateStatement + ") WHEN MATCHED THEN UPDATE SET"


updateColumns = dfColumns.split(",")
for lookupCol in updateColumns:
    updateStatement = updateStatement + targetTest + "." + lookupCol + "=" + statingTableTest + "." + lookupCol + ","
    #print("insertSQL={}".format(insertSQL))
    
remove=","
reverse_remove = remove[::-1]
insertLookupStatement = lookupStatement[::-1].replace(reverse_remove,"",1)[::-1] +");"

#insert Statement
insertStatement = mergeStatement + insertStatement

#create final statement
finalStatement = updateStatement + insertStatement


#write to staging Table
df.write \ 
.format("com.microsoft.sqlserver.jdbc.spark")\
.mode("overwrite")\
.option("truncate", "true")\
.option("url","jdbc:sqlserver:// <enter sqlservername>  .database.windows.net;databaseName=<enter db name>;")\
.option("dbtable", azureSqlStagingTable)\
.option("user", "<enter sql username>")\
.option("password", "<enter sql password>")\
.option("schemaCheckEnabled","false")\
.save()


