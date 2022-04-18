Q. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

A. Sparkify database will be used by Sparkify Advanced Analythics team to gain insights about user behaviour, likes, dislikes and also on popular songs etc. With these insights, Sparkify can better understand their customers and build future capabilities with confidence as that would be driven by facts and not assumptions. This data will open up new business opportunities and drive up customer engagement/satisfaction.

Q.How to run the Python scripts.
A.Python scripts are run using the terminal with syntax "python<x> <filename.py>", where x denotes the python version installed on your machine and filename is the python script name.

Q. An explanation of the files in the repository
A. create_tables.py - creates a connection to DB, drop/create tables.
sql_queries.py - captures all sql queries related to fact/dimension tables on the db. SQL queries related to create/insert/select are all written for import in other python scripts.
etl.py - Contains the code to extract data from the file path, transform based on the requirements of the project and then load it into POSTGRES SQL database. It picks up each file and then processes the data in that file. A total of 70 song files were loaded and 30 log files. The data was then transferred to relevant tables in the database.  
    
Q. State and justify your database schema design and ETL pipeline.
A. DB schema: It is a star schema design. We have 1 fact table with multiple dimension tables with 1:N relationship with fact table being in the centre and other dimensions table not having any further tables associated with them. Data is de-normalized, with simple sql queries and fast aggregations. I could not install the sqlalchemy package and draw up the ERD. I will continue to explore that and provide it in the next assignment.
    
ETL pipeline: Use song file to extract data for songs and artists table, then use log file to fill in data for time and users table. With the combination of songs, artists and log data we can then populate songplays table. We start with dimensions table first as some of the data required by songplays table can't be derived until we load the other dimensions table.
    
Q. [Optional] Provide example queries and results for song play analysis.
A. Please refer sql_queries.py script for example of sql_queries.
In terms of results please refer to 
    
songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent
0	2018-11-30 00:22:07.796000	91	free	None	None	829	Dallas-Fort Worth-Arlington, TX	Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)
    
    
