import pandas as pd
import requests
import json
import psycopg2
from sqlalchemy import create_engine



def get_data(file_url):
    """
        Pull data from url
        file_url: url to download data
    """
    try:
        # Get content
        response = requests.get(file_url)

        # Split the input into individual JSON objects
        json_objects = [json.loads(obj) for obj in response.text.strip().split('\n')]
        
        # Create df
        raw_data = pd.DataFrame(json_objects)   

        # Prepare df
        df = pd.json_normalize(raw_data['event'])

        # Extract type column
        df = pd.concat([df, raw_data[['id','type']]], axis=1)
        df = df.rename(columns={'customer-id': 'customer_id'})

        # Filter only logged in customers
        df = df[~df['customer_id'].isnull()]

        # Convert numeric cols
        df['customer_id'] = df['customer_id'].astype(int)
        df['id'] = df['id'].astype(int)

        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Sort data 
        df = df.sort_values(by=['customer_id','id','timestamp'])

        
    except Exception as e:
        print('\nError occured while downloading data')
        print(f'Error message: {e}')
        print('Exit...')
        exit()
    
    return df

def sessionize(ct_data):
    """
        Sessionize the data 
        ct_data: processed data 
    """

    # Create Session ID
    try:
        
        # for debug
        #ct_data = ct_data.query("`customer_id` in [2298, 3588]")
        #ct_data = df.copy()
        ct_data = ct_data.sort_values(by=['customer_id','id','timestamp'])

        # Count inactive minutes
        ct_data['inactive_time'] = ct_data.groupby('customer_id')['timestamp'].diff().dt.total_seconds() / 60

        # max inactive minutes
        ct_data['session_boundary'] = (ct_data['inactive_time'] > 30) | (ct_data['inactive_time'].isnull())

        # Assign session IDs
        ct_data['session_id'] = (ct_data['session_boundary']).cumsum()

        ct_data.drop('session_boundary', axis=1, inplace=True)
    except Exception as e:
        print('\nError occured while creating session_id')
        print(f'Error message: {e}')
        print('Exit...')
        exit()
    # Session info
    try:
        # Convert type column
        ct_data['type'] = ct_data['type'].astype(str)

        # collect distinct event types
        def collect_as_list(series):
            series = series.astype(str)
            return list(set(series))
        
        # Get session star-end and event types
        df_group = ct_data.groupby(['customer_id','session_id']).agg({'timestamp':['min', 'max'],
                                                    'type': collect_as_list})
        df_group.reset_index(inplace=True)
        df_group.columns = ['customer_id','session_id','session_start','session_end', 'type']


        # Session Duration
        df_group['session_duration'] = (df_group['session_end'] - df_group['session_start']).dt.total_seconds() / 60
        
        # Cumulative Session Duration for each customer
        df_group['session_duration_cum'] = df_group.groupby('customer_id')['session_duration'].cumsum()

        # Check if 'placed_order' is in the type list
        def check_order_placed(types):
            return 'placed_order' in types

        # Create order_placed column
        df_group['order_placed'] = df_group['type'].apply(check_order_placed)

        # Create session rank for each customer
        df_group['session_rank'] = df_group.groupby('customer_id')['session_id'].rank(method='first').astype(int)

        # Drop unnecessary cols
        df_group.drop('type', axis=1,inplace=True)
        
        
    except Exception as e:
        print('\nError occured while creating session info')
        print(f'Error message: {e}')
        print('Exit...')
        exit()

    return df_group

def insert_postgres(session_data):

    # try:
    #     connection = psycopg2.connect(
    #         host="localhost",
    #         database="omerliler",
    #         user="omerliler",
    #         password="J4nuary_23",
    #         port="5432"
    #     )
    # except Exception as e:
    #     print('\nError occured while creating connection to postgres db')
    #     print(f'Error message: {e}')

    try:
        session_data = session_data[['customer_id','session_id','session_start','session_end','session_duration','order_placed']]

        # connection string: postgresql://username:password@localhost:5432/database_name
        engine = create_engine('postgresql://omerliler:admin@localhost:5432/user_data')

        session_data.to_sql('session_data', engine, if_exists='replace', index=False)
    except Exception as e:
        print('\nError occured while inserting data into postgres table')
        print(f'Error message: {e}')
        print('Exit...')
        exit()

if __name__ == '__main__':

    print('\nDownloading Data!')
    raw_data = get_data('https://storage.googleapis.com/xcc-de-assessment/events.json')
    
    print('\nData downloaded!')
    print(f'Row count of raw data : {len(raw_data)}')
    print(f'Columns of raw data : {raw_data.columns.tolist()}')

    print('\nSessionizing Started!')
        
    session_data = sessionize(raw_data)

    print('\nSessionizing Finished!')
    print(session_data)

    print(f'\nRow count of session data : {len(session_data)}')
    print(f'Columns of session data : {session_data.columns.tolist()}')

    print('\nInserting into postgres table!')
        
    insert_postgres(session_data)

    print('\nInsert Finished!')
