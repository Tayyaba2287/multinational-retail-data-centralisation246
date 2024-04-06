
import pandas as pd
import yaml
import psycopg2
from tabula import read_pdf
from sqlalchemy import create_engine, inspect
import requests
import boto3
from urllib.parse import urlparse
from io import BytesIO

class DataExtractor:
    def __init__(self, creds_file):
        from database_utils import DatabaseConnector
        self.db_creds = DatabaseConnector.read_db_creds(creds_file)
        self.connection = self.init_db_connection()

    def init_db_connection(self):
        """
        Initializes and returns a psycopg2 connection using credentials.
        """
        conn = psycopg2.connect(
            dbname=self.db_creds['RDS_DATABASE'],
            user=self.db_creds['RDS_USER'],
            password=self.db_creds['RDS_PASSWORD'],
            host=self.db_creds['RDS_HOST'],
            port=self.db_creds.get('port', 5432) 
        )
        if conn:
            print("Database connection established successfully.")
        else:
            print("Failed to establish database connection.")
        return conn
    def read_rds_table(self, table_name):
        """
        Extracts the specified table from the RDS database to a pandas DataFrame.
        """
        conn = self.init_db_connection()
        query = f"SELECT * FROM {table_name};"
        return pd.read_sql_query(query, conn)
    
    def retrieve_pdf_data(self, link):
        """
        Extracts tables from a PDF document and returns them as a pandas DataFrame.

        Parameters:
        - link: The URL or file path to the PDF document.

        Returns:
        - A pandas DataFrame containing the extracted table data.
        """
        # Use tabula.read_pdf() to read tables from the PDF
        # 'pages' argument set to 'all' to extract from all pages
        dfs = read_pdf(link, pages='all', multiple_tables=True)
        
        # Combine all tables into a single DataFrame, if there are multiple tables
        if len(dfs) > 1:
            combined_df = pd.concat(dfs, ignore_index=True)
        else:
            combined_df = dfs[0] if dfs else pd.DataFrame()
        return combined_df
    
    @staticmethod
    def list_number_of_stores(endpoint, headers):
        """
        Returns the number of stores by calling the API endpoint.
        
        Parameters:
        - endpoint: The URL to retrieve the number of stores.
        - headers: A dictionary containing the header details for the request, including the API key.
        
        Returns:
        The number of stores as an integer.
        """
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get('number_stores')  
        else:
            print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
            return None

    @staticmethod
    def retrieve_stores_data(endpoint, headers):
        """
        Extracts all the stores from the API and saves them in a pandas DataFrame.
        
        Parameters:
        - endpoint: The base URL to retrieve each store's details. Assumes store number can be appended.
        - headers: A dictionary containing the header details for the request, including the API key.
        
        Returns:
        A pandas DataFrame containing the stores data.
        """
        num_stores = DataExtractor.list_number_of_stores(endpoint_number_of_stores, headers)
        stores_data = []
        for store_number in range(1, int(num_stores) + 1):
            store_response = requests.get(f"{endpoint}/{store_number}", headers=headers)
            if store_response.status_code == 200:
                stores_data.append(store_response.json())
            else:
                print(f"Failed to retrieve store {store_number}. Status code: {store_response.status_code}")
                print(f"Response body: {store_response.text}")
        return pd.DataFrame(stores_data)
    
    @staticmethod
    def extract_from_s3(s3_url):
        # Parse the S3 URL to get the bucket name and object key
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')

        # Initialize a boto3 client
        s3_client = boto3.client('s3')

        # Create a bytes buffer
        buffer = BytesIO()

        # Download the file from S3 directly into the buffer
        s3_client.download_fileobj(bucket_name, object_key, buffer)

        # Set the buffer's position to the beginning
        buffer.seek(0)

        # Assuming the file is CSV; use the appropriate pandas method if it's not
        data_frame = pd.read_csv(buffer)

        return data_frame
    
    @staticmethod
    def extract_from_s3_jason(s3_url):
        # Parse the S3 URL to get the bucket name and object key
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc.split('.s3')[0]
        object_key = parsed_url.path.lstrip('/')

        # Initialize a boto3 client
        s3_client = boto3.client('s3')

        # Create a bytes buffer
        buffer = BytesIO()

        # Download the file from S3 directly into the buffer
        s3_client.download_fileobj(bucket_name, object_key, buffer)

        # Set the buffer's position to the beginning
        buffer.seek(0)

        # Assuming the file is CSV; use the appropriate pandas method if it's not
        data_frame = pd.read_json(buffer)

        return data_frame

if __name__ == "__main__":

    #For the code starting with a hash is for testing out the data and seeing how it looks like in a dataframe. I have put it a hash so that I am not printing out the dataframes again and again.
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name1 = 'legacy_store_details'
    table_name2 = 'legacy_users'
    table_name3 = 'orders_table'
    df_legacy_store_details = extractor.read_rds_table(table_name1)
    df_legacy_users = extractor.read_rds_table(table_name2)
    df_orders_table = extractor.read_rds_table(table_name3)
    #print(df_orders_table)
    #print(df_orders_table[df_orders_table.isna().any(axis=1)])
    #mask = df_legacy_users[df_legacy_users['user_uuid'].str.len() == 10]
    #print(df_legacy_users['join_date'].apply(type).unique())
    #rows_with_missing = df[df_legacy_users.isna().any(axis=1)]
    #print(rows_with_missing)
    #print(df_orders_table)
    card_data =  extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #print(df_legacy_store_details)
    #rows_with_missing = card_data[card_data.isna().any(axis=1)]
    #print(rows_with_missing)

    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    endpoint_number_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    endpoint_retrieve_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    num_stores = DataExtractor.list_number_of_stores(endpoint_number_of_stores, headers)
    #print(f"Number of stores: {num_stores}")

    df_stores = DataExtractor.retrieve_stores_data(endpoint_retrieve_store, headers)
    #print(df_stores)
    # this data frame and data is the same exact as the legacy_store_data. so I will be using legacy_store_data as th other method gives me en error for atleast one of the stores.

    s3_url = 's3://data-handling-public/products.csv'
    df_products_data = DataExtractor.extract_from_s3(s3_url)
    #print(df_products_data)
    #rows_with_missing = df_products_data[df_products_data.isna().any(axis=1)]
    #print(rows_with_missing)

    s3_url_of_date_time_data = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df_date_time_data = DataExtractor.extract_from_s3_jason(s3_url_of_date_time_data)
    #print(df_date_time_data)
    #print(df_date_time_data[df_date_time_data.isna().any(axis=1)])
    #print(df_date_time_data[df_date_time_data['day'] == 'NULL'])
