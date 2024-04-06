
import pandas as pd
import numpy as np
from decimal import Decimal

if __name__ == "__main__":
    from data_extraction import DataExtractor
    extractor = DataExtractor("/Users/tayya/multinational-retail-data-centralisation246\db_creds.yaml")
    table_name1 = 'legacy_store_details'
    table_name2 = 'legacy_users'
    table_name3 = 'orders_table'
    df_legacy_store_details = extractor.read_rds_table(table_name1)
    df_legacy_users = extractor.read_rds_table(table_name2)
    df_orders_table = extractor.read_rds_table(table_name3)
    card_data =  extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    s3_url = 's3://data-handling-public/products.csv'
    df_products_data = DataExtractor.extract_from_s3(s3_url)
    s3_url_of_date_time_data = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df_date_time_data = DataExtractor.extract_from_s3_jason(s3_url_of_date_time_data)

class DataCleaning:
    print("DataCleaning class is being defined...")
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
    
    @staticmethod
    def clean_card_data(df):
        df.loc[:,'expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y' , errors='coerce').dt.strftime('%m/%Y')
        df.loc[:,'date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', errors='coerce').dt.date
        return df
    
    @staticmethod
    def clean_store_data(df):
        print("clean_store_data method is defined")
        """
        Cleans the legacy_store_details DataFrame of user data.
       
        - drops rows with NULL values in all columns
        - Removes rows with incorrect information. Specifically drops rows which only contain 10 character strings in all columns that have no meaning  
        - Corrects errors with dates and ensures correct format
        - Ensures data types are correct for each column
       
        Parameters:
        df : pandas.DataFrame
            The DataFrame containing user store data to clean.
       
        Returns:
        pandas.DataFrame
            The cleaned DataFrame.
        """
        df = df.loc[df['country_code'].str.len() <= 2]
        df.loc[:,'latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df.loc[:,'longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df.loc[:,'staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df.loc[:,'opening_date'] = pd.to_datetime(df['opening_date'], format='mixed').dt.date
        df.replace('N/A', pd.NA)
        df.replace('NULL', pd.NA)
        return df
    
    @staticmethod
    def convert_product_weights(df):
        def convert_to_kg(weight):
            weight_str = str(weight)
            
            # Extract numeric values and unit from the weight string
            numeric_part = ''.join(filter(str.isdigit, weight_str))
            unit_part = ''.join(filter(str.isalpha, weight_str)).lower()
            
            # Convert the numeric part to float
            if numeric_part:
                numeric_value = float(numeric_part)
            else:
                return None
              
            if unit_part in ['g', 'ml']:
                # Assuming 1ml = 1g, convert grams or ml to kg
                return numeric_value / 1000
            elif unit_part == 'kg':
                return numeric_value
            else:
                # Handle unknown or unsupported unit
                return numeric_value
        
        # Apply the conversion function to the 'weight' column
        df['weight'] = df['weight'].apply(convert_to_kg)
        
        return df
    
    @staticmethod
    def clean_products_data(df):
        df_copy = df.copy()
        df_copy.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        df_copy = df_copy.loc[df_copy['uuid'].str.len() != 10]
        df_copy.loc[:,'date_added'] = pd.to_datetime(df_copy['date_added'], format='mixed' , errors='coerce').dt.date
        df_copy['product_price'] = df_copy['product_price'].apply(lambda x: Decimal(str(x).lstrip('£')) if pd.notna(x) and str(x).lstrip('£').replace('.', '', 1).isdigit() else None if pd.isna(x) else x)
        df_copy.dropna(inplace=True)
        return df_copy
    
    @staticmethod
    def clean_orders_data(df):
        df_copy = df.copy()
        df_copy.drop(columns=['first_name', 'last_name', '1'], inplace=True)
        df_copy.loc[:,'product_quantity'] = pd.to_numeric(df['product_quantity'], errors='coerce')
        return df_copy
    
    @staticmethod
    def clean_date_time_data(df):
        df.replace('NULL', pd.NA)
        df = df.loc[df['timestamp'].str.len() != 10]
        df = df.loc[df['day'] != 'NULL']
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        return df

if __name__ == "__main__":
    cleaned_user_data = DataCleaning.clean_user_data(df_legacy_users)
    cleaned_card_data = DataCleaning.clean_card_data(card_data)
    cleaned_store_data = DataCleaning.clean_store_data(df_legacy_store_details)
    converted_products_weights_data = DataCleaning.convert_product_weights(df_products_data)
    cleaned_products_data = DataCleaning.clean_products_data(converted_products_weights_data)
    cleaned_orders_data = DataCleaning.clean_orders_data(df_orders_table)
    cleaned_date_time_data = DataCleaning.clean_date_time_data(df_date_time_data)

    #printing and testing out the cleaned data. Making sure its correct. 
    #print(cleaned_orders_data)
    #print(cleaned_orders_data[cleaned_orders_data.isna().any(axis=1)])
    #print(cleaned_products_data)
    #print(cleaned_products_data[cleaned_products_data.isna().any(axis=1)])
    #print(cleaned_date_time_data)