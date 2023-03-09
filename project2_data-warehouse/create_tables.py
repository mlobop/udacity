# LIBRARIES

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# AUXILIARY FUNCTIONS

def drop_tables(cur, conn):
    '''
    Purpose: drops all the fact and dimensions tables and also the staging tables
    Arguments:
        cur: cursor to execute queries
        conn: connection to the database
    ''' 
    
    # Drops all the tables in the database
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            # Success message
            print("Table dropped " + query)
        except psycopg2.Error as e:
            print("Error: The following table could not be dropped " + query)
    
def create_tables(cur, conn):
    '''
    Purpose: creates the star schema, if it does not exist, and the tables
        We will use a distribution strategy in this schema
        Besides, we will also create the staging tables
    Arguments:
        cur: cursor to execute queries
        conn: connection to the database
    '''
    
    # First, the star schema is created, in case it does not exist
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dist;")
        conn.commit()
    except psycopg2.Error as e:
        print("Error: The schema could not be created")
    
    # Next, all analytics and staging tables are created in our Sparkify database
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            # Success message
            print("Table created " + query)
        except psycopg2.Error as e:
            print("Error: The following table could not be created " + query)
    
# MAIN FUNCTION

def main():
    '''
    Purpose: connects to the database based upon the data included in the config file
        Then, it drops the existing table
        Finally, new fact and dimension tables, and also staging tables, are created
    Arguments: N/A
    Configuration parameters:
        - host: Redshift cluster endpoint
        - dbname: name of the DB -> dev
        - user: user of the DB Sparkify
        - password: password of the DB Sparkify
        - port: 5439
    '''
    
    # Reads the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connection to the DB
    try: 
        print("Let's try to connect")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        # Success message
        print("Connection to Sparkify database has been established successfully!")
    except psycopg2.Error as e: 
        print("Error: Could not establish connection to the Sparkify DB")
        print(e)
    
    # Runs all the DDL queries, i.e, every existing table in the database will be dropped and new ones will be created according to the business necessities of our application 
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # Closes the connection to the database
    conn.close()

# Execution of the script
if __name__ == "__main__":
    main()