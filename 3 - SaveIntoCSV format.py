# Databricks notebook source
#import sql functions
from pyspark.sql import functions as F

# COMMAND ----------

# try to mount source data cointainer storage
# should use secret scope on prod env
storageAccountName = "datashare123"
storageAccountAccessKey = "A0Oi1ApK/MNoLA04bsSE9UCsCqCrpY2MgU76x7swsMR22L0tPpJ7EGN9blZppEAxbySDlzIfvhjUDOpT0X4FMg=="
blobContainerName = "datashare"
try:
  dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = "/mnt/datashare/",
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      )
except:
  print("Already exist")

# COMMAND ----------

# read data and get the lastest data for each id and name
df = spark.sql("select *, row_number() over (partition by _id, nome order by DateExtraction desc, LoadDate desc) as rnk from registroenderecoescola")
df = df.filter("rnk == '1'")
df = df.drop("rnk").drop("LoadDate").withColumnRenamed("DateExtraction","UltimaAtualizacao")

# COMMAND ----------

filename = "dbfs:/mnt/datashare/Temp"
df.write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(path=filename)