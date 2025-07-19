# Databricks notebook source
dbutils.fs.mkdirs("/Volumes/workspace/ghcn/ghcnvol/raw/daily_files")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/ghcn/ghcnvol/bronze")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/ghcn/ghcnvol/silver")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/ghcn/ghcnvol/gold")