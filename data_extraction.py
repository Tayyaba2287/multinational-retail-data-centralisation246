
import pandas as pd
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
from database_utils import DatabaseConnector

class DataExtractor:

    def __init__(self, creds_file):
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

if __name__ == "__main__":
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name = 'legacy_store_details'
    df = extractor.read_rds_table(table_name)
    print(df)
    
    