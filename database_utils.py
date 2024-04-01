import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from urllib.parse import quote
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

if __name__ == "__main__":
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name2 = 'legacy_users'
    df_legacy_users = extractor.read_rds_table(table_name2)
if __name__ == "__main__":
    cleaned_user_data = DataCleaning.clean_user_data(df_legacy_users)

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
    
    original_password = '@Trifles8799'
    encoded_password = quote(original_password)
    db_user = 'postgres'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'sales_data'

    connection_string = f"postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
    table_name = 'dim_users'
    DatabaseConnector.upload_to_db(connection_string, cleaned_user_data, table_name)