# LIBRARIES

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# AUXILIARY FUNCTIONS

def load_staging_tables(cur, conn):
    '''
    Purpose: loads data from S3 json files into staging tables in Redshift
    Arguments:
        cur: cursor to execute queries
        conn: connection to the database
    '''

    # Copies data from S3: song data and log data
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            # Success message
            print("Data copied successfully into staging table" + query)
        except psycopg2.Error as e:
            print("Error: The following data could not be copied " + query)

def insert_tables(cur, conn):
    '''
    Purpose: selects data from staging tables and inserts into fact and dimension tables of the DB
    Arguments:
        cur: cursor to execute queries
        conn: connection to the database
    '''

    # Copies data from S3: song data anlog data
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            # Success message
            print("Data inserted successfully into final table" + query)
        except psycopg2.Error as e:
            print("Error: The following data could not be inserted " + query)
    
# MAIN FUNCTION

def main():
    '''
    Purpose: connects to the database based upon the data included in the config file
        Next, data is copied into Redshift
        Finally, data is transformed and loaded into the final tables to be queried for analytical purposes
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
    
    # Perform the ETL operations
    # First, data is copied from S3 into staging tables in Redshift
    # Next, data is transformed and loaded into the fact and dimension tables previously created
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    # Closes the connection to the database
    conn.close()

# Execution of the script
if __name__ == "__main__":
    main()