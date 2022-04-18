Q. Summary of the Project
A. In this project, we will be extracting JSON files (song and user activity log) located in a S3 bucket, copying them to Redshift and then creating analytics tables. These analytics tables once created can be queried to derive insights by Sparify Business and Data Analytics team. 

Q. How to run the Python scripts?
A. Python scripts description. These files need to be run in the following order.

1. sql_queries.py is a python script that contains queries to copy data from S3 to redshift, create db tables and then finally inserting records from staging to analytics tables. This is the first file that needs to be run.
2. create_tables.py is a python script that contains instructions on dropping tables and creating them.
3. etl.py has funtions to load staging tables and then inserting data from staging to analytics table.


Q. Explanation of the files in the repository
A. Files in the folder:
Schema.png: A schema drawing showin facts, dimensions tables and primary and foregin keys.
create_tables.py: A python script that contains instructions on dropping tables and creating them.
dwh.cwg: A config file that carries the db, IAM and S3 bucket details.
etl.py: A python script used to load staging tables and then inserting data from staging to analytics table.
sql_queries.py: A script that contains queries to copy data from S3 to redshift, create DB tables and then finally inserting records from staging to analytics tables.


Q: Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
A: This database will help sparkify data science/business teams to find out insights about their user listening habits/behaviours/likes/dislikes and help them make decisions based on DATA. The new database is a filtered/transformed copy of the actual data and would allow sparkify team unparalleld access to data they have neve had before.

Q: State and justify your database schema design and ETL pipeline.
A: Please refer to the star schema design 'png' file. We have one fact table and several dimensions table. Primary keys and foreign keys are well established.
