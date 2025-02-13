# Databricks notebook source
# MAGIC %md
# MAGIC ### Streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw_schema.streamsrc(
# MAGIC   id int,
# MAGIC   order_name string,
# MAGIC   amount double,
# MAGIC   prod_id int
# MAGIC ) using delta 
# MAGIC location 'abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/raw_schema/streamsrc'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_catalog.raw_schema.streamsrc values(4,"chocolate",10.00,1234),(5,"strawberry",15.00,1235),(6,"vanilla",20.00,1236),(7,"chocolate",10.00,1237),(8,"strawberry",15.00,1238),(9,"vanilla",20.00,1239)

# COMMAND ----------

df=spark.readStream.table('delta_catalog.raw_schema.streamsrc')

# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", "abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/streamsink/checkpoint").trigger(processingTime="60 seconds").start("abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/streamsink/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@deltalakeaccountstorage.dfs.core.windows.net/streamsink/data`