import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
    - Reads queries from a list and then drops existing tables on the database
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Create tables one by one, passed as a list of queries in the for loop
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Read the config file for connecting to the DB
    - Call the drop table function
    - Call the create table function
    - Close the connection
"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()