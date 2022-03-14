import configparser
import psycopg2
from table_queries import create_table_queries, drop_table_queries

# Drop existing tables
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Create new tables, staging, fact, and dimension 
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read redshift credentials
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    
    drop_tables(cur, conn)
    print("Droping tables complete.")
    create_tables(cur, conn)
    print("Creating tables complete.")
    
    conn.close()


if __name__ == "__main__":
    main()