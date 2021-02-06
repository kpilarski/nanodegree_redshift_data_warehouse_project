import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    function for loading data from S3 into staging tables
    
    parameters:
    1) cur: connection cursor to AWS REDSHIFT 
    2) conn: connection to the database to do the commit
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    function for inserting all data for final tables
    
    parameters:
    1) cur: connection cursor to AWS REDSHIFT 
    2) conn: connection to the database to do the commit
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function - running ETL Process
    """
    print('reading config...')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('connecting to database...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('loading staging data then inserting to data warehouse...')
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print('Data has been inserted')

if __name__ == "__main__":
    main()