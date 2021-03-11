# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# get the conn with SQL server
jdbchost = "devserverescola.database.windows.net"
jdbcport = "1433"
jdbcdatabase = "dev_database_escola"
properties = {
    "user": "sa-dev",
    "password": "Password@1"
}

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbchost,jdbcport,jdbcdatabase)

# COMMAND ----------

#create a view from spark table
dfs = spark.sql("select * from registroenderecoescola")

# COMMAND ----------

# create a view from target table 
dft = spark.read.jdbc(url=url, table="registroenderecoescola", properties=properties)

# COMMAND ----------

left_join = dfs.join(dft, (dfs._id == dft._id) & (dfs.nome==dft.nome) & (dfs.DateExtraction==dft.DateExtraction) ,how='leftanti')

# COMMAND ----------

# insert new records if any
if left_join.count() > 0:
  left_join.write.jdbc( url=url, table= "registroenderecoescola", mode= "append", properties=properties)
else:
  print ("No new records to insert")

# COMMAND ----------

