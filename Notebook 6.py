# Databricks notebook source
# MAGIC %md
# MAGIC ### **Delta Live Tables**

# COMMAND ----------

import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bronze streaming table

# COMMAND ----------

@dlt.table(
    name= "bronze_table"
)
def bronze_table():
    df=spark.readStream.table("delta_catalog.raw_schema.streamsrc")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver View

# COMMAND ----------

@dlt.view
def silver_table():
    df=spark.read.table("Live.bronze_table")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold mat view

# COMMAND ----------

@dlt.table
def gold_table():
    df=spark.read.table("Live.silver_table")
    return df