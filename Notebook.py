# Databricks notebook source
from pyspark.sql.functions import *;
from pyspark.sql.types import *;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists delta_catalog.raw_schema

# COMMAND ----------

# MAGIC %md
# MAGIC manage table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.manageorder_table(
# MAGIC   id int,
# MAGIC   order_name string,
# MAGIC   amount double,
# MAGIC   prod_id int
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.manageorder_table values(1,"chocolate",10.00,1234),(2,"strawberry",15.00,1235),(3,"vanilla",20.00,1236),(4,"chocolate",10.00,1237),(5,"strawberry",15.00,1238),(6,"vanilla",20.00,1239)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.manageorder_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### External delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.Extorder_table(
# MAGIC   id int,
# MAGIC   order_name string,
# MAGIC   amount double,
# MAGIC   prod_id int
# MAGIC ) using delta 
# MAGIC location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/Extorder_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.extorder_table values(1,"chocolate",10.00,1234),(2,"strawberry",15.00,1235),(3,"vanilla",20.00,1236),(4,"chocolate",10.00,1237),(5,"strawberry",15.00,1238),(6,"vanilla",20.00,1239)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/Extorder_table`

# COMMAND ----------

# MAGIC %md
# MAGIC ### CETAS(create external table as select)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog delta_catalog;
# MAGIC use schema raw_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(),current_schema();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists delta_catalog.raw_schema.CETAS_table;
# MAGIC create table delta_catalog.raw_schema.CETAS_table 
# MAGIC using delta location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/CETAS_table'
# MAGIC as
# MAGIC  select * from delta_catalog.raw_schema.extorder_table

# COMMAND ----------

dbutils.fs.ls('abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/Extorder_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.extorder_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep cloning

# COMMAND ----------

# MAGIC %sql
# MAGIC create table extorder_table_deep_clone 
# MAGIC DEEP CLONE delta_catalog.raw_schema.extorder_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shallow clone

# COMMAND ----------

# MAGIC %sql
# MAGIC create table mngorder_table_shallow_clone 
# MAGIC SHALLOW CLONE delta_catalog.raw_schema.manageorder_table