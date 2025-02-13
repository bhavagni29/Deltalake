# Databricks notebook source
# MAGIC %md
# MAGIC ### Schema **Evolution**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Schema

# COMMAND ----------

my_data=[(1,"Food",100),(2,"Clothing",200),(3,"Electronics",300)]
my_schema="id INT, category STRING, price int"
df=spark.createDataFrame(my_data,my_schema)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *;
from pyspark.sql.types import *;

# COMMAND ----------

df_new=df.union(spark.createDataFrame([(4,"Others",100)],my_schema))

# COMMAND ----------

df_new=df_new.withColumn("flag",lit(1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new.write.format("delta")\
.mode("append")\
.option("mergeSchema","true")\
.save("abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/schema_evl")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/schema_evl")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicit Schema updates
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding a column

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table delta_catalog.raw_schema.dml_table add columns Flag STRING

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.dml_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding a new Column After New

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table delta_catalog.raw_schema.dml_table add columns new_col STRING after id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.dml_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reordering the column

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table delta_catalog.raw_schema.dml_table Alter column Flag  after new_col

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.dml_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta_catalog.raw_schema.dml_table 
# MAGIC SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table delta_catalog.raw_schema.dml_table rename column Flag to newFlag

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.dml_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### REORG  

# COMMAND ----------

# MAGIC %sql
# MAGIC Reorg table delta_catalog.raw_schema.dml_table apply(purge)