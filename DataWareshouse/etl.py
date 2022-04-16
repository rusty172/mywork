import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
    - Load staging events & songs table with song and log JSON files.
    - Copy commands passed in the for loop for execution
"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Process insert function on all the analytics table, insert queries are passed individually in the for loop
"""

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Read the config file for connecting to the DB
    - Call the load staging table function
    - Call the insert query function
    - Close the connection
"""      

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()