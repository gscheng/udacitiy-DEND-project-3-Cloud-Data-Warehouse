import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
     CREATE TABLE staging_events (
         artist VARCHAR(150),
         auth VARCHAR(20),
         first_name VARCHAR(150),
         gender CHAR(1),
         itemInSession INTEGER,
         last_name VARCHAR(150),
         length FLOAT,
         level VARCHAR(20),
         location VARCHAR(100),
         method VARCHAR(10),
         page VARCHAR(50),
         registration BIGINT,
         sessionId SMALLINT,
         song VARCHAR,
         status SMALLINT,
         ts BIGINT,
         user_agent VARCHAR,
         user_id INTEGER
    );
""")


staging_songs_table_create= ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR(30),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INTEGER,
        song_id VARCHAR(30),
        title VARCHAR,
        year SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time(start_time), 
        user_id INTEGER REFERENCES users(user_id), 
        level VARCHAR, 
        song_id VARCHAR REFERENCES songs(song_id), 
        artist_id VARCHAR REFERENCES artists(artist_id), 
        session_id INTEGER, 
        location VARCHAR, 
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR(1), 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR NOT NULL, 
        year SMALLINT, 
        duration DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude FLOAT, 
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour SMALLINT, 
        day SMALLINT, 
        week SMALLINT, 
        month SMALLINT, 
        year SMALLINT, 
        weekday SMALLINT
    );
""")


# STAGING TABLES

# Load from JSON Arrays Using a JSONPaths file (LOG_JSONPATH),
# setting COMPUPDATE, STATUPDATE OFF to speed up COPY

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'  
    COMPUPDATE OFF STATUPDATE OFF
    JSON {}""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))
      

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2'   
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (e.ts / 1000) * interval '1 second',
        e.user_id, 
        e.level,
        s.song_id, 
        s.artist_id, 
        e.sessionId, 
        e.location, 
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s ON (s.artist_name = e.artist) AND (s.title = e.song) AND (s.duration = e.length)
    WHERE e.page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = user_id addAND s.start_time = start_time AND s.session_id = session_id )
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts / 1000) * interval '1 second' AS start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time), 
        EXTRACT(week from start_time), 
        EXTRACT(month from start_time), 
        EXTRACT(year from start_time), 
        EXTRACT(weekday from start_time)
    FROM staging_events
    WHERE page = 'NextSong'
    AND start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
