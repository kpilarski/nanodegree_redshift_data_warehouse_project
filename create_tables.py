import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3

def drop_tables(cur, conn):
    """
    function for dropping all tables if they exists
    
    parameters:
    1) cur: connection cursor to AWS REDSHIFT 
    2) conn: connection to the database to do the commit
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    function for creating all tables
    
    parameters:
    1) cur: connection cursor to AWS REDSHIFT 
    2) conn: connection to the database to do the commit
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function - creating DWH schema (drop and create tables)
    """
    print('reading config...')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('connecting to database...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('creating tables...')
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('Tables has been created')

if __name__ == "__main__":
    main()