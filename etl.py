import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """
    Copy data from S3 and loads them into staging tables
    """
    print('loading staging tables...')
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print('done loading')


def insert_tables(cur, conn):
    """
    Run SQL queries from the staging tables and loads them into analytics tables
    """
    print('inserting into tables...')
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print('done inserting')


def main():
    """
    Main Program
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    start_time1 = time.time()
    load_staging_tables(cur, conn)
    elapsed_time1 = time.time() - start_time1
    print('insert staging tables elapsed time: ', time.strftime("%H:%M:%S", time.gmtime(elapsed_time1)))
    
    start_time2 = time.time()
    insert_tables(cur, conn)
    elapsed_time2 = time.time() - start_time2
    print('insert tables elapsed time: ', time.strftime("%H:%M:%S", time.gmtime(elapsed_time2)))
    conn.close()


if __name__ == "__main__":
    main()