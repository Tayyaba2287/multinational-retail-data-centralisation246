import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

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