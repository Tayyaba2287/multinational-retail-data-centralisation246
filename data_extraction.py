
import pandas as pd
import yaml
import psycopg2
from tabula import read_pdf
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

if __name__ == "__main__":
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name1 = 'legacy_store_details'
    table_name2 = 'legacy_users'
    table_name3 = 'orders_table'
    df_legacy_store_details = extractor.read_rds_table(table_name1)
    df_legacy_users = extractor.read_rds_table(table_name2)
    df_orders_table = extractor.read_rds_table(table_name3)
    mask = df_legacy_users[df_legacy_users['user_uuid'].str.len() == 10]
    #print(df_legacy_users['join_date'].apply(type).unique())
    #rows_with_missing = df[df_legacy_users.isna().any(axis=1)]
    #print(rows_with_missing)
    #print(df_orders_table)
    card_data =  retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(card_data)
