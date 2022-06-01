1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
Purpose: A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to extract data from S3, process the data into analytics tables using Spark, and load them back into S3 so that the analytics team can analyze the data for advanced analytics. The advanced analytics team can draw insights from this data and then assist Sparkify leadership team to make well informed, data-driven decisions.


2. State and justify your schema design and ELT pipeline.
Using the song and log datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong. Partiotioned by year and month.
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database. Partioned by year and artist.
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units. Partitioned by year and month.
start_time, hour, day, week, month, year, weekday

ETL pipeline reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.

Steps to run: 
1. Ensure s3 output bucket is set up
2. Set up IAM user using 4 roles (S3ReadOnly, S3FullAccess, AdminAccess, RedshiftReadOnly)
3. Derive Access & Secret Key
4. Update the credentials file dl.cfg
5. Run script etl.py in the terminal

3. Provide example queries and results for song play analysis. (Refer to screenshot document)

