# Project - Data Lake

A music streaming startup, **Sparkify**, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Goal of the project - build an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back to S3 as a set of dimensional tables. This will allow analytics team to continue finding insights in what songs their users are listening to.

### Project Datasets


* Song data: `s3a://udacity-dend/song_data`
* Log data: `s3a://udacity-dend/log_data`

#### 1. Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. 

The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
###### *Example of file (TRAABJL12903CDCF1A.json):*
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, 
"artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", 
"song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", 
"duration": 152.92036, "year": 0}
```

#### 2. Log Dataset
The second dataset is log files in JSON format generated an event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month. For example, here are file paths to two files in this dataset.

```
     log_data/2018/11/2018-11-12-events.json
     log_data/2018/11/2018-11-13-events.json
```
###### *Example of file (2018-11-12-events.json):*

```
{"artist":"Slipknot", "auth":"LoggedIn", "firstName":"Aiden", "gender":"M",
"itemInSession":0,"lastName":"Ramirez", "length":192.57424,"level":"paid",
"location":"New York-Newark-Jersey City, NY-NJ-PA", "method":"PUT", 
"page":"NextSong", "registration":1540283578796.0,"sessionId":19,
"song":"Opium Of The People (Album Version)", "status":200, 
"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
"ts":1541639510796,"userId":"20"}
```


## Project structure

* `dl.cfg` - Config file with AWS credentials

```
        KEY=YOUR_AWS_ACCESS_KEY
        SECRET=YOUR_AWS_SECRET_KEY
```
* `etl.py` - Python script that extracts songs and log data from AWS S3, transforms it using Spark, and loads the created dimensional tables in parquet format back to S3.

* `README.md` - Current file, contains detailed information about the project.


## ETL Pipeline
1. Loads the credentials from `dl.cfg`
  
2. Reads data from S3.
    
            Song data: s3a://udacity-dend/song_data
            Log data:  s3a://udacity-dend/log_data
    

3. Processes loaded json data using Spark and creates Fact and Dimensional Tables

4. Saves generated tables in parquet format back to S3.

## Data Schema
### Fact Table

#### songplays - records in log data associated with song plays (records with page = "NextSong")
*partitionBy("year", "month")*


column name | type
------------- | -------------
songplay_id | long
start_time | timestamp 
user_id | string 
level | string 
song_id | string 
artist_id | string 
session_id | long 
location | string 
user_agent | string
month | integer
year | integer


### Dimension Tables

#### songs - songs in music database
*partitionBy("year", "artist_id")*


column name | type
------------- | -------------
song_id | string
title | string
artist_id | string
year | integer
duration | double


#### artists - artists in music database

column name | type
------------- | -------------
artist_id | string
artist_name | string
artist_location | string
artist_latitude | double
artist_longitude | double~


#### time - records from songplays with specific time units
*partitionBy("year", "month")*


column name | type
------------- | -------------
start_time | timestamp
hour | integer
day | integer
week | integer
month | integer
year | integer
weekday | integer

#### users - users in the app


column name | type
------------- | -------------
userId | string
firstName | string
lastName | string
gender | string
level | string






