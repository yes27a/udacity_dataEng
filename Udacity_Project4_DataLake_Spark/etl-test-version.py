import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType
from pyspark.sql import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This is ETL process for song data. It extract songs dimension table and artists dimension table from song data.
    """
    
    print(' ==== start process_song_data ==== ')
    # get filepath to song data file
    #song_data_path = input_data + 'song_data/*/*/*/*.json'
    song_data_path = input_data + 'song_data/A/A/*/*.json' # smaller set of song data
    
    # read song data file
    song_data = spark.read.json(song_data_path)
    song_data.printSchema()
    
    # extract columns to create songs table
    songs_table = song_data.select('song_id','title','artist_id','year','duration')
    
    # remove dupliated rows by song_id
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    print('--- start to write parquet file of songs table(dimension) ---')
    songs_table.limit(5).write.partitionBy('year','artist_id').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = song_data.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')
    
    # remove duplicated rows by artist_id
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print('--- start to write parquet file of artists table(dimension) ---')
    artists_table.limit(5).write.parquet(output_data + 'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """
    This is ETL process for log data. It extracts users dimension table and time dimension table and songplays fact table.
    """
    
    print(' ==== start process_log_data ==== ')
    # get filepath to log data file
    #log_data_path = input_data + 'log_data/*/*/*.json'
    log_data_path = input_data + 'log_data/2018/11/*.json' # smaller data set

    # read log data file
    log_data = spark.read.json(log_data_path)
    log_data.printSchema()
    
    # filter by actions for song plays
    log_data = log_data.filter(log_data.page == 'NextSong')

    # extract columns for users table
    users_table = log_data.select('userId','firstName','lastName','gender','level').dropDuplicates(['userId'])
    
    # write users table to parquet files
    print('--- start to write parquet file of users table(dimension) ---')
    users_table.write.parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df2 = log_data.withColumn('timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).date(), DateType())
    df2 = df2.withColumn('ts_date',get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df2.select(col('timestamp').alias('start_time'),\
                            month('ts_date').alias('month'), \
                            year('ts_date').alias('year'),\
                            dayofmonth('ts_date').alias('day'),\
                            hour('timestamp').alias('hour'),\
                            weekofyear('ts_date').alias('week'), \
                            date_format('ts_date', 'u').cast('int').alias('weekday')
                           ).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    print('--- start to write parquet file of time table(dimension)---')
    time_table.write.partitionBy('year','month').parquet(output_data + 'time.parquet')
    
    time_table.createOrReplaceTempView('times')

    # read in song data, artist data to use for songplays table
    song_df = spark.read.parquet('songs2.parquet') #song_df = spark.read.parquet(output_data + 'songs.parquet')
    song_df.createOrReplaceTempView('songs')
    artists_df = spark.read.parquet('artists2.parquet') #artists_df = spark.read.parquet(output_data + 'artists.parquet')
    artists_df.createOrReplaceTempView('artists')

    df2.createOrReplaceTempView('logs')
    # extract columns from joined song and log datasets to create songplays table 
    #songplay_id, start_time, user_id, level, session_id, location, user_agent, song_id, artist_id
    songplays_table = spark.sql("""
        SELECT all.*, t.year, t.month
        FROM (
            SELECT timestamp as start_time, userId, level, sessionid, location, useragent, song_id, artist_id
            FROM logs l JOIN (
                SELECT s.song_id,s.title,a.artist_id,a.artist_name,s.duration 
                FROM songs s JOIN artists a ON s.artist_id = a.artist_id
                ) sa ON trim(l.artist) = trim(sa.artist_name) and trim(l.song) = trim(sa.title) and l.length = sa.duration
                WHERE l.page='NextSong'
            ) all JOIN times t ON all.start_time = t.start_time
        ORDER BY start_time
    """)

    # add sequential ID column, songplay_id
    window = Window.orderBy(col('start_time'))
    songplays_table = songplays_table.withColumn('songplay_id',row_number().over(window))
    
    # write songplays table to parquet files partitioned by year and month
    print('--- start to write parquet file of songplays table(fact) ---')
    songplays_table.limit(5).write.partitionBy('year','month').parquet(output_data + 'songplays.parquet')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://myudacity/project_dataLake/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print(' ==== completed successfully all ETL process ==== ')

if __name__ == "__main__":
    main()
