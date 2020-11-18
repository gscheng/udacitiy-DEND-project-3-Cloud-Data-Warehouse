import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Execute SQL commands to drops all tables in Redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Execute SQL commands to create tables in Redshift
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Program
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Connecting to Redshift...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')
    
    drop_tables(cur, conn)
    
    print('Creating tables')
    create_tables(cur, conn)
    print('Done')
    
    conn.close()


if __name__ == "__main__":
    main()