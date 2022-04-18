import os
import glob
import psycopg2
import pandas as pd

from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads individual file to a dataframe based on the filepath provided
    
    - Extracts relevant fields and stores them to a variable data frame
        
    - Converts dataframe values to a list  
    
    - Insert data in dimension tables(song, artists)
    
    """    
    
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    song_data = song_data.values.tolist()[0]
    #print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = artist_data.values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads individual log file to a dataframe based on the filepath provided
    
    - Filter records based on page == 'NextSong'
    
    - Convert timestamp column to datetime
    
    - Insert time data to list time_data & define column labels
    
    - Combine data to create a time_df dataframe for insertion to time table
    
    - Load user data frame with relevant values from main log dataframe
    
    - Drop duplicates and null values
    
    - Insert records to user table
    
    - finally insert songplays records 
        
    - Converts dataframe values to a list  
    
    - Insert data in dimension tables(song, artists)
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[(df['page'] == 'NextSong')]

    # convert timestamp column to datetime
    df.loc[:,'ts'] = (pd.to_datetime(df['ts'], unit='ms'))
    
    # insert time data records
    time_data = [df["ts"].dt.time, df["ts"].dt.hour, df["ts"].dt.day, df["ts"].dt.weekofyear, df["ts"].dt.month, df["ts"].dt.year, df["ts"].dt.weekday]
    column_labels = ['start_time','hr','dd','WoY','mm','yy','weekday']
    
    #Marry up the above data to create a dictionary using ZIP
    time_data_dict=dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(time_data_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].copy()
    user_df=user_df.dropna()
    user_df=user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(users_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    - Read all files with the .JSON suffix in the directory
    
    - Get total number of files picked up from the folder & print the count
    
    - Iterate individually over files and process each file using functions process_song_file and process_log_file
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Establish connection with spakify db
    
    - define curosor object
    
    - call function process_data with relevant arguments
    
    - close connection
    
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
