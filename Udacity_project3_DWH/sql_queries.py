import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
log_jsonpath = config.get('S3','LOG_JSONPATH')
log_data_url = config.get('S3','LOG_DATA')
song_data_url = config.get('S3','SONG_DATA')
iam_role = config.get('IAM_ROLE','ARN_NAME')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(300)
    ,auth varchar(50)
    ,firstName varchar(100)
    ,gender char(1)
    ,itemInSession smallint
    ,lastName varchar(100)
    ,length numeric
    ,level char(4)
    ,location varchar(100)
    ,method varchar(10)
    ,page varchar(20)
    ,registration bigint
    ,sessionId int
    ,song varchar(200)
    ,status int
    ,ts bigint
    ,userAgent varchar(200)
    ,userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer
    ,artist_id char(18)
    ,artist_latitude numeric
    ,artist_longitude numeric
    ,artist_location varchar(300)
    ,artist_name varchar(300)
    ,song_id char(18)
    ,title varchar(200)
    ,duration numeric
    ,year smallint
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
   songplay_id int identity(0,1) primary key
   ,start_time timestamp references time(start_time)
   ,user_id int references users(user_id)
   ,level char(4)
   ,song_id char(18)
   ,artist_id char(18)
   ,session_id char(18)
   ,location varchar(200)
   ,user_agent varchar(200)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int primary key
    ,first_name varchar(200) not null
    ,last_name varchar(200) not null
    ,gender char(1)
    ,level char(4)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id char(18) primary key
    ,title varchar(200) not null
    ,artist_id char(18) not null
    ,year smallint
    ,duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
    artist_id char(18) primary key
    ,name varchar(300) not null
    ,location varchar(300)
    ,latitude numeric
    ,longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp primary key
    ,hour smallint not null
    ,day smallint not null
    ,week smallint not null
    ,month smallint not null
    ,year smallint not null
    ,weekday smallint not null
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
iam_role {}
json {} region 'us-west-2'
""").format(log_data_url, iam_role, log_jsonpath)

staging_songs_copy = ("""
COPY staging_songs from {}
iam_role {}
json 'auto' region 'us-west-2'
""").format(song_data_url, iam_role)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
timestamp 'epoch' + event.ts/1000 * interval '1 second' AS ts
,event.userId
,event.level
,dd.song_id
,dd.artist_id
,event.sessionId
,event.location
,event.userAgent
FROM staging_events event
JOIN (
  select so.song_id,ar.artist_id, ar.name, so.title
  from song so 
  JOIN artist ar ON so.artist_id=ar.artist_id
) dd ON trim(event.song)=dd.title and trim(event.artist)=dd.name
""")

user_table_insert = ("""
INSERT INTO users 
SELECT distinct userId,firstName,lastName,gender,level
FROM staging_events
WHERE userId is not null
""")

song_table_insert = ("""
INSERT INTO song  
SELECT song_id, title, artist_id, year, duration 
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist 
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time 
SELECT distinct ts2, hour, day, weekofyear, month, year, weekday
FROM (
    SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS ts2
        ,ts2::datetime as ts_date
        ,to_char(ts_date,'YYYY')::smallint as year
        ,to_char(ts_date,'MM')::smallint as month
        ,to_char(ts_date,'DD')::smallint as day
        ,to_char(ts_date,'HH24')::smallint as hour
        ,to_char(ts_date,'D')::smallint as weekday
        ,to_char(ts_date,'WW')::smallint as weekofyear
        FROM staging_events
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
