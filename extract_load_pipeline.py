import botocore, boto3, pandas as pd
import psycopg2
from botocore import UNSIGNED
from botocore.client import Config
import time
from sqlalchemy import create_engine
from config import config

print('All Libraries have been imported')

# Use the client
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name = 'eu-central-1')
print('Process 1 - Boto3 Client Creation Completed Sucessfully')

bucket =s3.list_objects_v2(Bucket='d2b-internal-assessment-bucket')

bucket_name = 'd2b-internal-assessment-bucket'

if bucket['Name'] == 'd2b-internal-assessment-bucket':
    print("Process 2 - The bucket exists")
else:
    print("Process 2 - The bucket does not exist")

print('Process 3 - Connection to AWS S3 is successful')

#Connect to Bucket containing json files
response = s3.list_objects_v2(Bucket='d2b-internal-assessment-bucket')

# Get Bucket Contents
files = response.get("Contents")
print('Process 4 - Files have been gathered')


def extract_load(path, schema, conn_param, table_name):
     csv_read = pd.read_csv(path)
     csv_read.to_sql(table_name, con=conn_param, if_exists='replace', index=False, schema=schema)

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
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
        print("Connection Created Successfully")
        return connection
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    for file in files:
    #Store file name as a variable
        table_name = str(file['Key'])  
    #check for csv files only
        if table_name.endswith('.csv') == True:
            if table_name.startswith('orders_data/orders') == True or  table_name.startswith('orders_data/reviews') == True or  table_name.startswith('orders_data/shipment_deliveries') == True:        
                print("Information - File is a csv: '{file}'".format(file=table_name))
                dataset = (table_name.split('/')[1]).split('.')[0]
                tab_name = str((table_name.split('/')[1]).split('.')[0])
                print(f'Process 5 - Treating Extraction and Loading for {tab_name}')
                #print(dataset)
                raw_path = f"s3://{bucket_name}/{table_name}"
                print(f'Information - Path: {raw_path}')
                extract_load(raw_path,'oluwseko5931_staging', connect(), tab_name)
                print(f'Final - Process Completed Successfully for {tab_name}.csv')
    
    print('Process Completed Successfully')

