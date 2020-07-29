import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This will be loading all the staging tables with values.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This funcytion  loads the final tables on start schema.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    This is the main function and the entry point,execution will start with this module
    and workflow moves to other functions.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print('Tables have been loaded')


if __name__ == "__main__":
    main()