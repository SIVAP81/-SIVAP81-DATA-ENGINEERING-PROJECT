
  Objective is to work on data engineering project for one of the big corporation's employees data from the 1980s and 1995s. All the database of employees from that period are provided six CSV files. In this project, I have designed data model with all the tables to hold data, imported the CSVs into a SQL database, transfered SQL database to HDFS/Hive, and performed analysis using Hive/Impala/Spark/SparkML using the data and created data and ML pipelines.


Importing data from MySQL RDBMS to HDFS using Sqoop, Creating HIVE Tables with compressed file format (avro), Explanatory Data Analysis with Impala & SparkSQL and Building Random Forest Classifer Model & Logistic Regression Model using SparkML.


#Run the followingcommand in order
Step-1
#mysql file 

mysql.sql

#shell file 

sh sqoop.sh

#hive file .sql extension

hive -f hiveproject.sql

spark file .py extenstion

spark-submit sparkml.py
