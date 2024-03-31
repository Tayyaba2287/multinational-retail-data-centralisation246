import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector

class DatabaseConnector:

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

    def init_db_engine(creds_file):
        """
        Initializes and returns an SQLAlchemy database engine using credentials from a YAML file.

        Parameters:
        - creds_file: The path to the YAML file containing the database credentials.

        Returns:
        - An SQLAlchemy engine connected to the specified database.
        """
        creds = read_db_creds(creds_file)
        if creds is None:
            print("Failed to read database credentials.")
            return None

        # Construct the database connection string
        db_url = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}/{creds['database']}"
        
        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
        return engine

    def list_db_tables(engine):
        inspector = Inspector.from_engine(engine)
        tables = inspector.get_table_names()
        print("Tables in the database:", tables)
        return tables
    