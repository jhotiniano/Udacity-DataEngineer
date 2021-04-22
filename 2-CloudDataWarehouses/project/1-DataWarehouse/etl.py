import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data from S3 to Redshift tables. These payloads live in the 'copy_table_queries' list.
    
    INPUT:
    * cur, the cursor object 
    * conn, the database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function loads data from stage tables for the definition of the star schema. These payloads live in the 'insert_table_queries' list.
    
    INPUT:
    * cur, the cursor object 
    * conn, the database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function connects to the Redshift database to load data from s3 and then define the star schema.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()