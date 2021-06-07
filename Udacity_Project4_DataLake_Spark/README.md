# Data Lake ETL for Sparkify music service log
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
This project is building an ETL pipeline that extracts their data from S3, and transforms data into a set of dimensional tables and a fact table using Spark. We don't need to create star schema tables because it's possible to do ELT(Extract, Load and Transform) in the Spark. These tables set is loaded back into S3 for their analytics team to continue finding insights in what songs their users are listening to. 
There's more detail in the following.

### Log data/Song database structure
   
- Song data: This is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. And it places in the cloud which is S3. Song dataset's json files looks like following. One json file contain a song info.

```sh
 {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
   
- Log data: This is activity log data from a music streaming app, Sparkify. A log json file contains multiple activity and it also locates in the cloud, S3. It looks like the following picture.

    ![log data json](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)

- Log data json path: This is metadata for the log data file above. 

### Database Schema

**Fact Table**

- songplays: records in event data associated with song plays i.e. records with page NextSong   
{songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}

**Dimension Tables**

- users: users in the app   
{user_id, first_name, last_name, gender, level}
- songs: songs in music database   
{song_id, title, artist_id, year, duration}
- artists: artists in music database   
{artist_id, name, location, lattitude, longitude}
- time: timestamps of records in songplays broken down into specific units   
{start_time, hour, day, week, month, year, weekday}

### ETL Process

1. Original data is in the S3 in the form of JSON. Load song data/log data from S3 into Spark Dataframe. 
2. Brief analysis is available with Spark dataframe. Filter columns that need to be included into the dimensional table and drop duplicated data.
3. Join log data with extracted dimension tables and load user's activity data into the fact table. 
4. These dimension tables and fact table are still in the Spark dataframe. Load these Spark dataframe into the S3 using Spark functions.

### What files are included

- etl.py: This file include not only all ETL process of both songs data and log data but also include making connection to S3 and creating and initiating Spark session.
- README.md: provide discussion on the process and decisions for this ETL pipeline.
- dl.cfg: Two parameters value are defined to connect to the S3 which are AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.


### How to run the script files

Open your terminal. Just run the python script, etl.py, to run the ETL code.   
<tt> >> python etl.py</tt>
<br/>When this script run, it will show the each steps it is executing. 




