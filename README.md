# Project 4: Data Lake


## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

 Building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project

In this project, the goal is to apply Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, the steps are as follows:
1. Load data from S3.
2. Process the data into analytics tables using Spark.
3. Load them back into S3. 
4. Deploy this Spark process on a cluster using AWS.

## Files

- etl.ipynb: reads data from S3, processes that data using Spark, and writes them back to S3
- test.ipynb: test some codes before starting etl.py
- dl.cfg: contains the AWS credentials

## Datasets
 The two datasets reside in S3. Here are the S3 links for each:
 ```
 Song data: s3://udacity-dend/song_data
 Log data: s3://udacity-dend/log_data
 ```


### Songs metadata

The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### User activity logs

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what a single activity log in 2018-11-13-events.json, looks like.

```
{"artist":null,"auth":"Logged In","firstName":"Kevin","gender":"M","itemInSession":0,"lastName":"Arellano","length":null,"level":"free","location":"Harrisburg-Carlisle, PA","method":"GET","page":"Home","registration":1540006905796.0,"sessionId":514,"song":null,"status":200,"ts":1542069417796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.125 Safari\/537.36\"","userId":"66"}
```

## ETL Processes

### Songs metadata

### Dimensional Tables

#### 1: songs table

- songs - songs in music database

| songs | | |
|---|---|---|
song_id | StringType | PRIMARY KEY
title | StringType
artist_id | StringType
year | TimestampType
duration | DoubleType


#### 2: artists table
- artists - artists in music database

| artists | | |
|---|---|---|
artist_id | StringType | PRIMARY KEY
artist_name | StringType
artist_location | StringType
artist_latitude | DecimalType
artist_longitude | DecimalType

### User activity logs

#### 3: time table
-  time - timestamps of records in songplays broken down into specific units
| time | | |
|---|---|--|
start_time | TimestampType | PRIMARY KEY
hour | IntegerType
day | IntegerType
week | IntegerType
month | IntegerType
year | IntegerType
weekday | IntegerType


#### 4: users table
- users - users in the app

| users | | |
|---|---|---|
user_id | StringType | PRIMARY KEY
first_name | StringType
last_name | StringType
gender | StringType
level | StringType

### Fact Table

#### 5: songsplays table
- songplays - records in log data associated with song plays i.e. records with page NextSong

| songplays | | |
|---|---|---|
songplay_id | IntegerType | PRIMARY KEY
start_time | TimestampType | FOREIGN KEY
user_id | StringType | FOREIGN KEY
level | StringType
song_id | StringType | FOREIGN KEY
artist_id | StringType | FOREIGN KEY
session_id | StringType
location | StringType
user_agent | StringType




## Usage

Simply run the ETL script.

```
$ python etl.py
```
