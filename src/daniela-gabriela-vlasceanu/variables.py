# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS daniela_vlasceanu_books")
working_dir = "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/"
azure_storage = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/"
