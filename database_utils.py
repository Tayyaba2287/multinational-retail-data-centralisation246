import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from urllib.parse import quote


if __name__ == "__main__":
    from data_extraction import DataExtractor
    from data_cleaning import DataCleaning
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name1 = 'legacy_store_details'
    table_name2 = 'legacy_users'
    df_legacy_store_details = extractor.read_rds_table(table_name1)
    df_legacy_users = extractor.read_rds_table(table_name2)
    cleaned_user_data = DataCleaning.clean_user_data(df_legacy_users)
    cleaned_store_data = DataCleaning.clean_store_data(df_legacy_store_details)
    card_data =  extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    cleaned_card_data = DataCleaning.clean_card_data(card_data)
    s3_url = 's3://data-handling-public/products.csv'
    df_products_data = DataExtractor.extract_from_s3(s3_url)
    converted_products_weights_data = DataCleaning.convert_product_weights(df_products_data)
    cleaned_products_data = DataCleaning.clean_products_data(converted_products_weights_data)
    table_name3 = 'orders_table'
    df_orders_table = extractor.read_rds_table(table_name3)
    cleaned_orders_data = DataCleaning.clean_orders_data(df_orders_table)
    s3_url_of_date_time_data = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df_date_time_data = DataExtractor.extract_from_s3_jason(s3_url_of_date_time_data)
    cleaned_date_time_data = DataCleaning.clean_date_time_data(df_date_time_data)
    

class DatabaseConnector:

    @staticmethod
    def read_db_creds(filename):
        """
        Read database credentials from a YAML file.

        Parameters:
        - filename: The path to the YAML file containing the credentials.

        Returns:
        - A dictionary containing the database credentials, or None if an error occurs.
        """
        try:
            with open(filename, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            return None

    @staticmethod
    def init_db_engine(creds_file):
        """
        Initializes and returns an SQLAlchemy database engine using credentials from a YAML file.

        Parameters:
        - creds_file: The path to the YAML file containing the database credentials.

        Returns:
        - An SQLAlchemy engine connected to the specified database.
        """
        creds = DatabaseConnector.read_db_creds(creds_file)
        if creds is None:
            print("Failed to read database credentials.")
            return None

        # Construct the database connection string
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}/{creds['RDS_DATABASE']}"
        
        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
        return engine

    @staticmethod
    def list_db_tables(engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("Tables in the database:", tables)
        return tables
    
    @staticmethod
    def upload_to_db(connection_string, df, table_name, if_exists='replace', index=False):
        """
        Uploads a pandas DataFrame to a specified table in the database.
        """
        engine_2 = create_engine(connection_string)
        if engine_2 is not None:
            df.to_sql(name=table_name, con=engine_2, if_exists=if_exists, index=index)
            print(f"DataFrame uploaded successfully to '{table_name}' table.")
        else:
            print("Failed to create the database engine.")
    

if __name__ == "__main__":
    creds_file = 'db_creds.yaml'
    

    creds = DatabaseConnector.read_db_creds(creds_file)
    if creds is None:
        print("Could not read database credentials.")
    else:
        print("Database credentials successfully read.")
    

    engine = DatabaseConnector.init_db_engine(creds_file)
    if engine is None:
        print("Failed to initialize database engine.")
    else:
        print("Database engine successfully initialized.")
    
    
    if engine:
        tables = DatabaseConnector.list_db_tables(engine)
        print(f"Tables in the database: {tables}")
    else:
        print("Cannot list tables without an initialized database engine.")
    
    original_password = '' #I have left this blank as it contains my password. I could encrypt it but because of the danger that anything could be decrypted, I decided to ommit it. I also have the all the dataframes uploaded. 
    encoded_password = quote(original_password)
    db_user = 'postgres'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'sales_data'

    connection_string = f"postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
    table_name_1 = 'dim_users'
    table_name_2 = 'dim_card_details'
    table_name_3 = 'dim_store_details'
    table_name_4 = 'dim_products'
    table_name_5 = 'orders_table'
    table_name_6 = 'dim_date_times'

    #I have put the following in hash so that python doesn't keep on replacing the tables.
    #DatabaseConnector.upload_to_db(connection_string, cleaned_user_data, table_name_1)
    #DatabaseConnector.upload_to_db(connection_string, cleaned_card_data, table_name_2)
    #DatabaseConnector.upload_to_db(connection_string, cleaned_store_data, table_name_3)
    #DatabaseConnector.upload_to_db(connection_string, cleaned_products_data, table_name_4)
    #DatabaseConnector.upload_to_db(connection_string, cleaned_orders_data, table_name_5)
    #DatabaseConnector.upload_to_db(connection_string, cleaned_date_time_data, table_name_6)