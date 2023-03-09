# LIBRARIES
import configparser

# CONFIG

# In the config file we include the data of the created Redshift cluster and
# the IAM role ARN that gives access to Redshift to read data from S3
config = configparser.ConfigParser()
config.read('dwh.cfg')

# We set the required parameters to be used here
# Apart from ARN, we need the path of the data to be copied from S3
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

# These are the statements required to drop all the tables in the database and the staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Here we include all the statements to create all the tables in the database and the staging tables
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT SORTKEY DISTKEY,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id VARCHAR SORTKEY DISTKEY,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL DISTKEY,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id VARCHAR NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT NOT NULL,
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    ) diststyle all;
""")

# STAGING TABLES

# These are the queries to load data from S3 into staging tables on Redshift
staging_events_copy = ("""
    COPY staging_events from {} 
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs from {} 
    credentials 'aws_iam_role={}'
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

# With these queries we insert data into the fact and dimension analytics tables defined previously
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
        e.userId AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs s
        ON e.artist = s.artist_name
            AND e.song = s.title
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        e.userId AS user_id,
        e.firstName As first_name,
        e.lastName AS last_name,
        e.gender AS gender,
        e.level AS level
    FROM staging_events e
    WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        s.song_id AS song_id,
        s.title AS title,
        s.artist_id AS artist_id,
        s.year AS year,
        s.duration AS duration
    FROM staging_songs s
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        s.artist_id AS artist_id,
        s.artist_name AS name,
        s.artist_location AS location,
        s.artist_latitude AS latitude,
        s.artist_longitude AS longitude
    FROM staging_songs s
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(ISODOW FROM start_time)
    FROM staging_events e
    JOIN staging_songs s
        ON e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

# QUERY LISTS

# These lists include all the queries to be executed all at once for the different processes: drop and create tables (from create_tables.py) and copy and insert data (from etl.py)
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]