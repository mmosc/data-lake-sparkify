import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


from pyspark.sql.types import StructType, StructField, \
DoubleType as Dbl, LongType as Long, StringType as Str, \
     IntegerType as Int, DecimalType as Dec, DateType as Date, \
     TimestampType as Stamp

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session.
    
    return: spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def create_song_schema():
    """
    Create schema for song data.
    
    return: schema
    """
    song_schema = StructType([
        StructField("num_songs", Int()),
        StructField("artist_id", Str()),
        StructField("artist_latitude", Dec()),
        StructField("artist_longitude", Dec()),
        StructField("artist_location", Str()),
        StructField("artist_name", Str()),
        StructField("song_id", Str()),
        StructField("title", Str()),
        StructField("duration", Dbl()),
        StructField("year", Int())
    ])
    return song_schema


def create_log_data():
    """
    Create schema for log data.
    
    return: schema
    """
    log_schema = StructType([
        StructField("artist", Str()), 
        StructField('auth', Str()),
        StructField('firstName', Str()),
        StructField('gender', Str()),
        StructField('itemInSession', Int()),
        StructField('lastName', Str()),
        StructField('length', Dbl()),
        StructField('level', Str()),
        StructField('location', Str()),
        StructField('method', Str()),
        StructField('page', Str()),
        StructField('registration', Dec()),
        StructField('sessionId', Int()),
        StructField('song', Str()),
        StructField('status', Int()),
        StructField('ts', Long()),
        StructField('userAgent', Str()),
        StructField('userId', Int())
    ])
    return log_schema

def process_song_data(spark, input_data, output_data):
    """
    Process song data by creating songs and artists table
    and writing the result to a given S3 bucket.
    
    """
    # get filepath to song data file
    song_data_path = input_data + "song_data/*/*/*/*.json"
    print("Start processing song_data JSON files...")
    
    start_sd = datetime.now()
    # read song data file
   
    print("Reading song_data files from {}...".format(song_data_path))
    
    df_song_data = spark.read.json(song_data_path, schema = create_song_schema())
    
    stop_sd = datetime.now()
    total_sd = stop_sd - start_sd
    print("...finished reading song_data in {}.".format(total_sd))
    
    print("Song_data schema:")
    df_song_data.printSchema()
    
    
    # extract columns to create songs table
    print("Extracting columns to create songs table...")

    songs_table = df_song_data.select('song_id',
                                      'title', 
                                      'artist_id',
                                      'year', 
                                      'duration').dropDuplicates(['song_id'])

    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table to parquet files partitioned by year and artist...")
    
    songs_table.write.parquet(output_data + "songs_table.parquet", 
                              partitionBy = ["year", "artist_id"],
                              mode = "overwrite")

    # extract columns to create artists table
    print("Extracting columns to create artists table...")
    artists_table = df_song_data.select('artist_id', 
                                        'artist_name', 
                                        'artist_location', 
                                        'artist_latitude', 
                                        'artist_longitude').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("Writing artists table to parquet files...")
    artists_table.write.parquet(output_data + "artists_table.parquet", 
                                mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process log data by creating songs and artists table
    and writing the result to a given S3 bucket.
    
    """
    # get filepath to log data file
    log_data_path = input_data + "log_data/*/*/*.json"
    
    print("Start processing log_data JSON files...")
    start_ld = datetime.now()
    # read song data file
   
    print("Reading log_data files from {}...".format(log_data_path))

    # read log data file
    df_log_data = spark.read.json(log_data_path, schema = create_log_data())
    
    print("Show log_data schema:")
    df_log_data.printSchema()
    
    # filter by actions for song plays
    df_log_data = df_log_data.filter(df_log_data.page == 'NextSong')
    
    stop_ld = datetime.now()
    total_ld = stop_ld - start_ld
    print("...finished reading log_data in {}.".format(total_ld))
    

    # extract columns for users table
    print("Extracting columns for users table...")
    users_table = df_log_data.select('userId', 
                                     'firstName', 
                                     'lastName', 
                                     'gender', 
                                     'level').dropDuplicates(["userId"])
    
    # write users table to parquet files
    print("Writing users table to parquet files...")
    users_table.write.parquet(output_data + "users_table.parquet",
                             mode = "overwrite")

    # create timestamp column from original timestamp column
    print("Create timestamp column from original timestamp column...")
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df_log_data = df_log_data.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    print("Create datetime column from original timestamp column...")                   
    get_datetime = udf(lambda x : datetime.fromtimestamp((x / 1000)), Stamp())
    df_log_data = df_log_data.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df_log_data.selectExpr("timestamp as start_time",
                                        "hour(timestamp) as hour",
                                        "dayofmonth(timestamp) as day",
                                        "weekofyear(timestamp) as week",
                                        "month(timestamp) as month",
                                        "year(timestamp) as year",
                                        "dayofweek(timestamp) as weekday"
                                        ).dropDuplicates(["start_time"])
    
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table to parquet files...")
    time_table.write.parquet(output_data + "time_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")

    
    # read in song data to use for songplays table
    song_df_path = input_data + "song_data/A/A/A/*.json"
    df_song_data = spark.read.json(song_df_path)
    
    df_song_data.createOrReplaceTempView("song_data")
    df_log_data.createOrReplaceTempView("log_data")
    # extract columns from joined song and log datasets to create songplays table 
    print("Extracting columns from joined song and log datasets to create songplays table...")
    songplays_table = spark.sql("""
    SELECT monotonically_increasing_id() as songplay_id,
       ld.timestamp as start_time,
       year(ld.timestamp) as year,
       month(ld.timestamp) as month,
       ld.userid, 
       ld.level, 
       sd.song_id, 
       sd.artist_id, 
       ld.sessionid, 
       ld.location, 
       ld.useragent
       
       FROM song_data sd 
       JOIN log_data ld 
       ON (sd.title = ld.song AND 
           ld.artist = sd.artist_name)
       AND ld.page = 'NextSong'
""")
 
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table to parquet files...")
    songplays_table.write.parquet(output_data + "songplays_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-sparkify-uda/"
    

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
