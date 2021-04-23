import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    This function creates a session in Spark and loads the liberia to work with Amazon S3.
    """
    
    # create and return SparkSession
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_data, output_data):
    """
    This function takes data from song_data and uses it to create the songs and artists tables.
    Also, use temporary tables to use SparkSQL and transform as needed more comfortably.
    
    INPUT:
    * spark the session spark
    * song_data the song_data file
    * out_data the output path
    """
    
    
    # Read and get the necessary columns for the songs table
    # Write data in a partitioned form (year, artist_id) in the songs table
    df_song_data = spark.read.json(song_data)
    df_song_data.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
        SELECT  distinct
                song_id
                ,title
                ,artist_id
                ,year
                ,duration
        FROM    songs_table
        ORDER BY song_id
    """)
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")
    
    
    # Gets the columns required for the artists table
    # Write data in the artists table
    df_song_data.createOrReplaceTempView("artists_df")
    artists_table = spark.sql("""
        SELECT  distinct
                artist_id as artist_id
                ,artist_name as name
                ,artist_location as location
                ,artist_latitude as latitude
                ,artist_longitude as longitude
        FROM    artists_df
        ORDER BY artist_id desc
    """)
    songs_table.write.mode("overwrite").parquet(output_data + "artists/")
    
    
    return songs_table, artists_table

def process_log_data(spark, log_data, song_data, output_data):
    """
    This function takes data from song_data / log_data and uses it to create the users, time and songplays tables.
    Also, use udf and temporary tables to use SparkSQL and transform as needed more comfortably.
    
    INPUT:
    * spark the session spark
    * log_data the log_data file
    * song_data the song_data file
    * out_data the output path
    """
    
    
    # Read, filter and get the necessary columns for the users table
    # Write data to the users table
    df_long_data = spark.read.json(log_data)
    df_long_data_filtered = df_long_data.filter(df_long_data.page == 'NextSong')
    df_long_data_filtered.createOrReplaceTempView("users_df")
    users_table = spark.sql("""
        SELECT  distinct
                userId as user_id
                ,firstName as first_name
                ,lastName as last_name
                ,gender
                ,level
        FROM    users_df
        ORDER BY last_name
    """)
    users_table.write.mode("overwrite").parquet(output_data + "users/")
    
    
    # Formats the ts field to the date and time data type
    # Gets the columns required for the dates table
    # Write data in partitioned form (year, month) in the dates table
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    df_long_data_filtered = df_long_data_filtered.withColumn("timestamp", get_timestamp("ts"))
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    df_long_data_filtered = df_long_data_filtered.withColumn("datetime", get_datetime("ts"))
    df_long_data_filtered.createOrReplaceTempView("time_df")
    time_table = spark.sql("""
        SELECT  distinct
                datetime as start_time
                ,hour(timestamp) as hour
                ,day(timestamp) as day
                ,weekofyear(timestamp) as week
                ,month(timestamp) as month
                ,year(timestamp) as year
                ,dayofweek(timestamp) as weekday
        FROM    time_df
        ORDER BY start_time
    """)
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")
    
    
    # Read and get the necessary columns (by joining the song data) for the songplays table
    # Write data in a partitioned form (year, month) in the songplays table
    df_songdata = spark.read.json(song_data)
    df_songdata.createOrReplaceTempView("song_data")
    songplays_table = spark.sql("""
        SELECT  monotonically_increasing_id() as songplay_id
                ,to_timestamp(l.ts/1000) as start_time
                ,month(to_timestamp(l.ts/1000)) as month
                ,year(to_timestamp(l.ts/1000)) as year
                ,l.userId as user_id
                ,l.level as level
                ,s.song_id as song_id
                ,s.artist_id as artist_id
                ,l.sessionId as session_id
                ,l.location as location
                ,l.userAgent as user_agent
        FROM    users_df l
                JOIN song_data s ON l.artist = s.artist_name and l.song = s.title
    """)
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays/")
    
    
    return users_table, time_table, songplays_table

def main():
    """
    This function executes the entire pipeline process. Takes data from song_data and log_data (on Amazon S3).
    It processes and writes them back to Amazon S3.
    """
    
    # spark session
    spark = create_spark_session()
    
    # inputs
    song_data = config['AWS']['S3_SONG_DATA']
    log_data = config['AWS']['S3_LOG_DATA']
    output_data = config['AWS']['S3_OUTPUT']
    
    # functions
    songs_table, artists_table = process_song_data(spark, song_data, output_data)
    users_table, time_table, songplays_table = process_log_data(spark, log_data, song_data, output_data)
    
if __name__ == "__main__":
    main()