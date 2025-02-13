# Databricks notebook source
# MAGIC %md
# MAGIC ### DML with Delta Lakes
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.DML_table(
# MAGIC   id int,
# MAGIC   order_name string,
# MAGIC   amount double,
# MAGIC   prod_id int
# MAGIC ) using delta 
# MAGIC location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/DML_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.DML_table values(1,"chocolate",10.00,1234),(2,"strawberry",15.00,1235),(3,"vanilla",20.00,1236),(4,"chocolate",10.00,1237),(5,"strawberry",15.00,1238),(6,"vanilla",20.00,1239)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Turn off Deletion vectors
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta_catalog.raw_schema.DML_table SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %md
# MAGIC ### update
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta_catalog.raw_schema.DML_table set amount = 30.00 where prod_id = 1234;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw_schema.DML_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from delta_catalog.raw_schema.DML_table where id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel & versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta_catalog.raw_schema.DML_table

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta_catalog.raw_schema.DML_table version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe history delta_catalog.raw_schema.DML_table

# COMMAND ----------

# MAGIC %sql
# MAGIC Restore delta_catalog.raw_schema.DML_table to version as of 4

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta_catalog.raw_schema.DML_table

# COMMAND ----------

# MAGIC %md 
# MAGIC ####To remove partitions which are not in used--->Vaccum
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.retentionDurationcheck.enabled=false
# MAGIC  vaccum <tablename> Retain 0 HOURS DRY RUN 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize &Z-ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.extorder_table values(166,"chocolate",10.00,1234),(81,"strawberry",15.00,1235),(91,"vanilla",20.00,1236),(103,"chocolate",10.00,1237),(111,"strawberry",15.00,1238),(121,"vanilla",20.00,1239)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize delta_catalog.raw_schema.extorder_table zorder by (prod_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.liq_cluster_table(
# MAGIC   id int,
# MAGIC   prod_name string,
# MAGIC   price double
# MAGIC )
# MAGIC using delta
# MAGIC location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/liq_cluster_table'
# MAGIC CLUSTER BY (id)