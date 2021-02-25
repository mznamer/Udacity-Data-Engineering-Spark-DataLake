import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, weekofyear, dayofweek, dayofmonth, hour, minute
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function loads the songs data files (song_data) from S3, processes them and
    creates songs and artists tables from these data, then loaded them to S3.
    parameters:
        spark - a spark session instance
        input_data - location of song_data json files
        output_data - output S3 bucket for saving processed data in dimensional tables 
    """
    
    # filepath to song data file
    song_data = "{}song_data/A/B/C/*.json".format(input_data)
    
    # reading song data file
    df_songs = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_songs.select("song_id", "title", "artist_id", "duration", "year").distinct()
       
    # write songs table to parquet files partitioning by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs/"), mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df_songs.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists/"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Function loads the event log file (log_data) from S3, extracts data and
    creates time, users and songplays tables from these data, then loaded them to S3.
    
    parameters:
        spark - a spark session instance
        input_data - location of log_data json files
        output_data - output S3 bucket for saving processed data in dimensional tables
    """
    
    #  filepath to log data file
    log_data = "{}log_data/2018/11/*-events.json".format(input_data)

    # read log data file
    df_logs = spark.read.json(log_data).distinct()
    
    # filter by actions for song plays
    df_logs = df_logs.filter(df_logs.page == "NextSong")

    # extract columns for users table    
    users_table = df_logs.select("userId","firstName","lastName","gender","level").distinct()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")
   
    # create datetime column from timestamp column
    convert_to_datetime = udf(lambda x : datetime.fromtimestamp(int(x)/1000), TimestampType())
    df_logs = df_logs.withColumn("start_time", convert_to_datetime("ts"))
    
    time_table = df_logs.select("start_time").distinct()
    
    # extract parts of datetime for creating time table
    time_table = time_table\
                .withColumn("year",year("start_time"))\
                .withColumn("month",month("start_time"))\
                .withColumn("week",weekofyear("start_time"))\
                .withColumn("weekday",dayofweek("start_time"))\
                .withColumn("day",dayofmonth("start_time"))\
                .withColumn("hour",hour("start_time"))\
                .withColumn("minute",minute("start_time"))
       
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(os.path.join(output_data, "songs/"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_logs.withColumn("songplay_id", monotonically_increasing_id())\
                      .join(df_songs, df_logs.song == df_songs.title, how='inner')\
                      .select(
                        "songplay_id",
                        "start_time",
                        year('start_time').alias('year'),
                        month('start_time').alias('month'),
                        col("userId").alias("user_id"),
                        "level",
                        "song_id",
                        "artist_id",
                        col("sessionId").alias("session_id"),
                        "location",
                        col("userAgent").alias("user_agent")
                      ).distinct()


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://2021-udacity-project-data-lake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
