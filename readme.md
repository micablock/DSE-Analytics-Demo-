### Some Features on DSE analytics

- DSEFS, similar to HDFS, no complexity, no master slave 
- DSEFS : uses cases such as data ingestion, staging, state management etc. 
- Workers and Executors read data on local node, leading to less data shuffling. 
- Manages Spark Master -auto restart
- Inherits DSE Security
- Integrates with DSE Search/Graph
- “Always on SQL” to connect with BI tools (JDBC/ODBC)


## USE CASE 1 : Loading data from DSEFS To Spark and process it


## Directory link
dsefs://10.101.32.140:5598/newdirectory/measurement_summary_1.csv

## Load data into spark

val df = spark.read.format("csv").option("inferSchema","True").load("dsefs://10.101.32.140:5598/newdirectory/measurement_summary_1.csv").toDF("measurement_date","station_code","address","co","latitude","longitude","no2","o3","pm10","pm2_5","so2")

## Check Schema
df.schema 

## Check Data
df.show()

## Basic filter 

df.filter(df("station_code") === 101).show(5)

## Basic filter
df.filter(df("station_code") === 101).count()

## Basic filter 
df.filter(df("co") > 35).count()

## Find max co in each station_code 

df.groupBy("station_code").agg(max("co")).show()

## Save to Cassandra CQL.
df.write.cassandraFormat(keyspace="test_data",table="measurement_summary").save()

create keyspace test_data WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'Analytics': '3', 'Cassandra': '3' };

create table test_data.measurement_summary (
    measurement_date text,
    station_code int,
    address text,
    latitude float,
    Longitude float,
    SO2 float,
    NO2 float,
    O3 float,
    CO float,
    PM10 float,
    PM2_5 float,
    primary key((station_code),measurement_date)
);

# Run on Another Node 

select * from measurement_summary limit 5;

## USE CASE2 : LOAD DATA FROM DSE CASSANDRA TO DSE ANLAYTICS and process it. 
I created dummy data almost 10M records using Cassandra Stress for this purpose. 

# Load from C* to SPARK

val df1 = spark.sql("select * from keyspace1.standard1")

df1.count()

df1.select("key","c0").show(5)

