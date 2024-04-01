from data_extraction import DataExtractor
import pandas as pd
import numpy as np

if __name__ == "__main__":
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name1 = 'legacy_store_details'
    table_name2 = 'legacy_users'
    table_name3 = 'orders_table'
    df_legacy_store_details = extractor.read_rds_table(table_name1)
    df_legacy_users = extractor.read_rds_table(table_name2)
    df_orders_table = extractor.read_rds_table(table_name3)

class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        """
        Cleans the legacy_user DataFrame of user data.

        - Removes rows with incorrect information. Specifically drops rows which only contain 10 character strings in all columns that have no meaning  
        - Corrects errors with dates and ensures correct format
        - Ensures data types are correct for each column
        
        Parameters:
        df : pandas.DataFrame
            The DataFrame containing user data to clean.
        
        Returns:
        pandas.DataFrame
            The cleaned DataFrame.
        """
        df = df.loc[df['user_uuid'].str.len() != 10]
        df.loc[:, 'date_of_birth'] = df.loc[:, 'date_of_birth'].replace("NULL", np.nan)
        df.loc[:, 'join_date'] = df.loc[:, 'join_date'].replace("NULL", np.nan)
        df.loc[:,'date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed' , errors='coerce').dt.date
        df.loc[:,'join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce').dt.date
        return df

if __name__ == "__main__":
    cleaned_user_data = DataCleaning.clean_user_data(df_legacy_users)
    print(cleaned_user_data)
    print(cleaned_user_data.info())