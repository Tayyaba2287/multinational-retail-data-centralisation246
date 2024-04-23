# multinational-retail-data-centralisation246

## Project Overview 
In a hypothetical scenario for practice, I'm tasked with addressing a challenge faced by a multinational company involved in global sales. The issue at hand is the dispersion of sales data across various platforms, complicating access and analysis. My project aims to centralize this data into a single database, streamlining access and serving as the singular source for sales information. This initiative will enable me to pull and assess up-to-date business metrics efficiently, facilitating more data-driven decision-making in this simulated business environment. I will also develop a starbased schema of the database, ensuring that the columns are of the correct data types.

## What I learnt
1. I learnt how to extract data from different platforms using different libraries and methods. It took a lot of trial and error but I made it eventually. 
2. I learnt how to clean data. I learnt how I should deal with NULL values and make the right datatypes of columns. I also learnt how to drop/delete invalid values without loosing information or modifying the original data. 
3. A common issue that was arising during extracting, cleaning and uploading the data was circular imports. I learnt that I should reorganize my code so that the import statement is not at the top, rather under the definition or method or in the "if __name__ == "__main__":" statement. This helped to remove any circular import errors. 
4. Another issue that arose was during the extracting stage of the store data, where I had to extract the data from a API to connect and retrieve data. This was difficult as I kept making silly errors which in turn gave me errors such as error status 500. I learnt how to debug these issues by printing out the error code, so that I know from which side the error is occuring. I also used if and then statement in my code to figure out what exactly is going wrong with my code. 
5. I learnt how to connect with my local postgresql database and upload panda data frames as tables inside the sales_db database.
6. I learnt how to create the star_based schema.

## Installation instructions
To run the project locally you can clone the project using git. Copy and paste the following command, making sure you navigate to a desired place in your computer where you want to clone the project.

git clone https://github.com/Tayyaba2287/multinational-retail-data-centralisation246.git

## Usage instructions
Once you clone the project, open it in your preferred IDE, i.e. VS Code, PyCharm etc.
There's no need to run the db_utils.py file since AWS credentials are needed. The database_utils.py file only demonstrates how the download of the dataset has been executed. 

## File structure of the project
**data_extraction.py file:**
This script hosts the DataExtractor class, functioning as a utility for data retrieval across diverse sources. It's designed with specific methods to facilitate the extraction of data from CSV files, APIs, and S3 buckets, catering to a range of data storage formats.

**database_utils.py file:** 
In this file, the DatabaseConnector class is defined to manage database interactions. Its primary purpose is to establish connections to a database and handle the uploading of data, ensuring seamless data integration into the designated storage system.

**data_cleaning.py file:** 
This script introduces the DataCleaning class, equipped with various methods aimed at purifying the data extracted from the aforementioned sources. It focuses on refining the data by removing inconsistencies and standardizing formats, thus preparing it for analysis or storage.

**Note:**
at the end of each of these files, you will see codes in hash. That's because I either didn't want python to repeat the results again or I was testing the dataframes to get more familiar with what's inside the data.

**Changing_data_types.sql file:**
In this file, I have made several modifications to enhance the database schema based on the star schema design:

Data Type Corrections: I have updated the data types of the columns across all tables to ensure accuracy and consistency.
Key Constraints: I have added primary key constraints to uniquely identify each record in the tables. Additionally, foreign key constraints have been implemented to maintain the relationships between tables.

By making these changes, I have structured the database to adhere to the principles of a star schema, which optimizes query performance and simplifies reporting. This ensures that all columns now have the appropriate data types and relationships are clearly defined through key constraints.

**querying_data.sql file:**
In this file I queried the data to get up to date metrics. 

**results_from_querying_data.csv file:**
In this file I have pasted the results from querying the data.

## License information
This repository is open-source and available for download. It showcases my proficiency with Python as a programming language, particularly in utilizing its data analysis libraries for exploratory data analysis (EDA). All libraries used in this repository are open-source.
