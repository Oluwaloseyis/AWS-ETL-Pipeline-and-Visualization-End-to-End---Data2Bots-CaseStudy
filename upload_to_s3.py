import botocore, boto3, pandas as pd
import psycopg2
from botocore import UNSIGNED
from botocore.client import Config
import time
from sqlalchemy import create_engine
from config import config

print('All Libraries have been imported')

bucket_name = 'd2b-internal-assessment-bucket'

list_tables = ['agg_public_holiday','agg_shipments', 'best_performing_product']

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    connection = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        #SQLAlchemy Connection String
        conn_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

        engine = create_engine(conn_string)

        connection = engine.connect()
        
        # create a cursor
        cur = conn.cursor()
        
	    #execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	    #close the communication with the PostgreSQL
        cur.close()
        print("Connection Created Successfully")
        return connection
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


def upload_final_tables(list_tables, connection, schema_name):
    for table in list_tables:
        df = pd.read_sql_table(table, connection, schema=schema_name)
        print(f'{table} read is successful')
        print(df.head())
        upload_path = f's3://{bucket_name}/analytics_export/oluwseko5931/{table}.csv'
        df.to_csv(upload_path, index=False)
        print('Upload is Complete')

upload_final_tables(list_tables, connect(), 'oluwseko5931_analytics')