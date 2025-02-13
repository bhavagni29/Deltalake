# Databricks notebook source
# MAGIC %md
# MAGIC ### DML with Delta Lakes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Deletion vector

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.DMLDV_table(
# MAGIC   id int,
# MAGIC   order_name string,
# MAGIC   amount double,
# MAGIC   prod_id int
# MAGIC ) using delta 
# MAGIC location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/DMLDV_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.DMLDV_table values(1,"chocolate",10.00,1234),(2,"strawberry",15.00,1235),(3,"vanilla",20.00,1236),(4,"chocolate",10.00,1237),(5,"strawberry",15.00,1238),(6,"vanilla",20.00,1239)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Turn off Deletion vectors
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### update
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_catalog.raw_schema.DMLDV_table set amount = 90.00 where prod_id = 1238;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.DMLDV_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from delta_catalog.raw_schema.DML_table where id = 1