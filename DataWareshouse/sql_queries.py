import configparser

"""
    - Reads the config file and loads necessary parameters for further use
    - Returns the connection and cursor to sparkifydb
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
    - DROP tables if they exist in the database
"""
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"


"""
    - Create staging tables to extract data from S3 to Redshift and other tables for the project
"""
staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events
(artist varchar,
 auth varchar,
 fname varchar,
 gender char,
 item_in_session smallint,
 lname varchar,
 length float,
 level varchar,
 location varchar,
 method varchar,
 page varchar,
 registration varchar,
 session_id int,
 song varchar,
 status smallint,
 ts bigint,
 user_agent varchar,
 userid smallint)
 """

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs
(num_songs int,
 artist_id varchar,
 artist_latitude float,
 artist_longitude float,
 artist_location varchar,
 artist_name varchar distkey,
 song_id varchar,
 title varchar,
 duration float,
 year int)
 """

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay
(
 songplay_id int IDENTITY(1,1),
 start_time timestamp NOT NULL sortkey,
 user_id INT distkey,
 level VARCHAR NOT NULL,
 song_id VARCHAR NOT NULL,
 artist_id VARCHAR NOT NULL,
 session_id INT NOT NULL,
 location VARCHAR NOT NULL,
 user_agent VARCHAR NOT NULL,
 PRIMARY KEY (songplay_id),
 FOREIGN KEY(user_id) REFERENCES users(user_id),
 FOREIGN KEY(song_id) REFERENCES songs(song_id),
 FOREIGN KEY(artist_id) REFERENCES artist(artist_id)) 
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users
(user_id smallint NOT NULL distkey PRIMARY KEY,
 first_name varchar NOT NULL,
 last_name varchar NOT NULL,
 gender varchar NOT NULL,
 level varchar NOT NULL)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY,
 title varchar NOT NULL,
 artist_id varchar NOT NULL,
 year int NOT NULL,
 duration float)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist
(artist_id varchar NOT NULL distkey PRIMARY KEY,
 name varchar NOT NULL,
 location varchar NOT NULL,
 latitude varchar NOT NULL,
 longitude varchar NOT NULL)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time_table
(
 time_id int IDENTITY(1,1) PRIMARY KEY,
 start_time timestamp NOT NULL,
 hour smallint NOT NULL,
 day smallint NOT NULL,
 week smallint NOT NULL,
 month smallint NOT NULL,
 year smallint NOT NULL,
 weekday int NOT NULL)
"""


"""
    - Use copy command to copy data from S3 song and log files and load them to staging tables in REDSHIFT
"""

staging_events_copy = """
                       copy staging_events from {}
                       credentials 'aws_iam_role={}'
                       json {}
                       region 'us-west-2';
                      """.format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3", "LOG_JSONPATH"))
    

staging_songs_copy = """
                       copy staging_songs from {} 
                       credentials 'aws_iam_role={}'
                       json 'auto'
                       region 'us-west-2';
                       """.format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

"""
    - FINAL tables required for the project, load data using staging tables
"""

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
                TIMESTAMP WITHOUT TIME ZONE 'epoch' + (se.ts / 1000) * INTERVAL '1 second',
                CAST(se.userid AS smallint), 
                se.level, 
                ss.song_id, 
                ss.artist_id, 
                se.session_id, 
                se.location, 
                se.user_agent
FROM staging_events AS se 
JOIN staging_songs AS ss
ON se.artist = ss.artist_name 
AND se.song = ss.title 
AND se.length = ss.duration
WHERE se.page = 'NextSong';                         
""")

user_table_insert = ("""
INSERT INTO users
(
user_id, 
first_name, 
last_name, 
gender, 
level
)
SELECT DISTINCT se.userid, se.fname, se.lname, se.gender, FIRST_VALUE(se.level IGNORE NULLS)
OVER (
      PARTITION BY se.userid ORDER BY se.ts DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     )
FROM staging_events as se
WHERE se.page = 'NextSong' AND se.userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT into songs
SELECT DISTINCT song_id,title,artist_id,year,duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT into artist
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
AND artist_name IS NOT NULL
AND artist_location IS NOT NULL
AND artist_latitude IS NOT NULL
AND artist_longitude IS NOT NULL
""")

time_table_insert = ("""
INSERT into time_table (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT sp.start_time,
                EXTRACT(hour FROM sp.start_time),
                EXTRACT(day FROM sp.start_time),
                EXTRACT(week FROM sp.start_time),
                EXTRACT(month FROM sp.start_time),
                EXTRACT(year FROM sp.start_time),
                EXTRACT(weekday FROM sp.start_time)
FROM songplay as sp;
""")


"""
    - List of queries required in create_tables and etl programs
"""

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

