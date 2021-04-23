# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


# Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


# S3 DataLake


## Prerequisites

1.**Python 3.x**
2.**Conda** and **PySpark**


## S3 buckets (Personal):

Song Data Path: s3://myproject-udacity4/song_data <br>
Log Data Path: s3://myproject-udacity4/log_data 

<b>Schema </b>

A Star Schema for optimized queries

<b>Fact Table</b>

<b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users</b> - users in the app. Columns: user_id, first_name, last_name, gender, level

<b>songs</b> - songs in music database. Columns: song_id, title, artist_id, year, duration

<b>artists</b> - artists in music database. Columns: artist_id, name, location, lattitude, longitude

<b>time</b> - timestamps of records in songplays broken down into specific units. Columns: start_time, hour, day, week, month, year, weekday


## Pipeline

1.Load the Data which are in JSON Files(Song Data and Log Data) <br>
2.Use Spark to process these JSON files <br>
3.Then generate a set of Fact and Dimension Tables <br>
4.Load back this data to S3


## Execution Steps:
1.Insert AWS IAM credential (access_key, secret_access) in dl.cfg. <br>
2.Run etl.py in terminal. Ej. python3 etl.py
