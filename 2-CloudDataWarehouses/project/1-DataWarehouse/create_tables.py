import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function removes tables from the 'drop_table_queries' list defined in the file 'sql_queries.py'.
    
    INPUT:
    * cur, the cursor object 
    * conn, the database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates tables from the list 'create_table_queries' defined in the file 'sql_queries.py'.
    
    INPUT:
    * cur, the cursor object 
    * conn, the database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function establishes the connection to the redshift cluster. You have the necessary credentials in the 'dwh.cfg' file.
    Reset the definition of the star model. Remove and create the tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()