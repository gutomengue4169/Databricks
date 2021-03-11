# Databricks notebook source
#import sql functions
from pyspark.sql import functions as F

# COMMAND ----------

# try to mount source data cointainer storage
storageAccountName = "datalakestorageescola"
storageAccountAccessKey = "kZoGlxzygrJ1ZV1TlM0LMmubkWR5WOk6AOnV8FSJ+iNpePhVXSRUo1az6gWxvlIntRK8Yo654zUbikM9jrBmIA=="
blobContainerName = "datalakesourcedata"
try:
  dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = "/mnt/datalakesourcedata/",
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      )
except:
  print("Already exist")

# COMMAND ----------

# find the latest folder to use as last extraction date
file_type = "json"
folder_source_name = (max(dbutils.fs.ls("/mnt/datalakesourcedata/registro_escolas"))).name
file_location = "dbfs:/mnt/datalakesourcedata/registro_escolas/" + folder_source_name

# COMMAND ----------

# read the file from storage
df = spark.read.format(file_type).option("inferSchema", "true").option("multiline" , "true" ).option("charset", "UTF-8").load(file_location)

# COMMAND ----------

# get only the main columns
df2 = df.select(F.col("result.*"))

# COMMAND ----------

# from json convert into table
explodeArrarDF = df2.withColumn('test',F.explode(F.col('records')))

# COMMAND ----------

# save into a new dataframe and add a column timestamp
dfReadSpecificStructure = explodeArrarDF.select("test.*").withColumn("LoadDate",F.current_timestamp()).withColumn("DateExtraction", F.lit(folder_source_name))

# COMMAND ----------

# create a view to make sure no duplicates are inserted 
dfReadSpecificStructure.createOrReplaceTempView("RecordSourceData")

# COMMAND ----------

#join the view and new table and only insert new records
dfReadSpecificStructure = sqlContext.sql("select RecordSourceData.* from RecordSourceData left join registroenderecoescola on RecordSourceData._id = registroenderecoescola._id and RecordSourceData.nome = registroenderecoescola.nome and RecordSourceData.LoadDate = registroenderecoescola.LoadDate where registroenderecoescola._id is null")

# COMMAND ----------

#insert new records if any
dfReadSpecificStructure.write.mode("append").saveAsTable("registroenderecoescola")