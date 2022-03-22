#Create a DROP Table Query List

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"             
                      


#Create a CREATE Table Query List
songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays
                            (songplay_id SERIAL PRIMARY KEY NOT NULL,
                             start_time timestamp,
                             user_id varchar,
                             level varchar,
                             song_id varchar,
                             artist_id varchar,
                             session_id int,
                             location varchar, 
                             user_agent varchar)
                        """
user_table_create = """CREATE TABLE IF NOT EXISTS users
                            (user_id varchar PRIMARY KEY NOT NULL, 
                             fname text, 
                             lname text,
                             gender text,
                             level varchar)
                     """
song_table_create = """CREATE TABLE IF NOT EXISTS songs
                            (song_id varchar PRIMARY KEY NOT NULL, 
                             title varchar, 
                             artist_id varchar, 
                             year int, 
                             duration float)
                     """
artist_table_create = """CREATE TABLE IF NOT EXISTS artists
                             (artist_id varchar PRIMARY KEY NOT NULL,
                              name text, 
                              location varchar,
                              latitude float, 
                              longitude float)
                        """
time_table_create = """CREATE TABLE IF NOT EXISTS time
                             (start_time time, 
                              hour int, 
                              day int,
                              week int, 
                              month int, 
                              year int, 
                              weekday int)
                    """


#Insert Queries
song_table_insert = """INSERT INTO songs(song_id, title, artist_id, year, duration)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (song_id) DO NOTHING"""

artist_table_insert = """INSERT INTO artists(artist_id,name,location,latitude,longitude)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (artist_id) DO NOTHING"""

time_table_insert = """INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)"""

users_table_insert = """INSERT INTO users(user_id,fname,lname,gender,level)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) DO NOTHING"""

songplay_table_insert = """INSERT INTO songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (songplay_id) DO NOTHING"""

#SELECT query
song_select = """SELECT songs.song_id,artists.artist_id
                 FROM songs 
                 JOIN artists 
                 ON songs.artist_id = artists.artist_id
                 WHERE songs.title = %s
                 AND   artists.name = %s
                 AND   songs.duration = %s"""


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]