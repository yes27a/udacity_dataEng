# Data Warehouse ETL for Sparkify music service log
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
This project is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables and a fact table for their analytics team to continue finding insights in what songs their users are listening to. 
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

**Staging Tables**
- staging song: same structure with the json file
- staging event: same structure with the json file   

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

1. create staging database and dimentional tables and fact tables on the Redshift. (if they exists already, drop them first.)
2. load song data/log data from S3 to staging tables on Redshift without transforming. Using COPY command to load it from JSON files.
3. load data from staging to dimention tables on Redshift. It is insert SQL using select so that transforming logic is included.
4. load data from staging to fact table on Redshift. It is insert SQL using select so that transforming logic is included.

### What files are included

- create_table.py: It drop or create staging tables and fact and dimension tables for the star schema in Redshift.
- etl.py: It load data from S3 into staging tables on Redshift and then load data from staging into the analytics tables on Redshift. data transformed when the data move to the analytics database.
- sql_queries.py: It define SQL statements such as create tables, drop tables, insert tables and COPY commands.
- README.md: provide discussion on the process and decisions for this ETL pipeline.
- dwh.cfg: parameters setting for connecting to the database and cloud environment.
- Configure_AWS.ipynb: It configure AWS environments and set up connection to it through code. This launch AWS Redshift and delete the cluster.
- dwh2.cfg: parameters setting for running Configure_AWS.ipynb

### How to run the script files

Launch Redshift cluster when if the cloud environment is not always active.
Open your terminal. First run the python script, create_tables.py, to create or reset the databases of staging and analytic environment.   
<tt> >> python create_tables.py</tt>
<br/>This script will not show any result. That's ok. Second, run the python script, etl.py, to run the ETL code.   
<tt> >> python etl.py</tt>
<br/>When this script run, it will show the sql codes it is executing. 




