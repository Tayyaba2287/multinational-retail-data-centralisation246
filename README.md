# multinational-retail-data-centralisation246

## Project Overview 
In a hypothetical scenario for practice, I'm tasked with addressing a challenge faced by a multinational company involved in global sales. The issue at hand is the dispersion of sales data across various platforms, complicating access and analysis. My project aims to centralize this data into a single database, streamlining access and serving as the singular source for sales information. This initiative will enable me to pull and assess up-to-date business metrics efficiently, facilitating more data-driven decision-making in this simulated business environment. I will also develop a starbased schema of the database, ensuring that the columns are of the correct data types.

## What I learnt
1. I learnt how to extract data from different platforms using different libraries and methods. It took a lot of trial and error but I made it eventually. 
2. I learnt how to clean data. I learnt how I should deal with NULL values and make the right datatypes of columns. I also learnt how to drop/delete invalid values without loosing information or modifying the original data. 
3. A common issue that was arising during extracting, cleaning and uploading the data was circular imports. I learnt that I should reorganise my code so that the import statement is not at the top, rather under the definition or method or in the "if __name__ == "__main__":" statement. This helped to remove any circular import errors. 
4. Another issue that arised was during the extracting stage of the store data, where I had to extract the data from a API to connect and retrieve data. This was difficult as i kept making silly errors which in turn gave me errors such as error status 500. I learnt how to debug these issues by printing out the error code, so that I know from which side the error is occuring. I also used if and then stement in my code to figure out what exactly is going wrong my code. 
5. I learnt how to connect with my local postgresql database and upload panda data frames as tables inside the sales_db database.

## Installation instructions

## Usage instructions

## File structure of the project
**data_extraction.py file:**
This script hosts the DataExtractor class, functioning as a utility for data retrieval across diverse sources. It's designed with specific methods to facilitate the extraction of data from CSV files, APIs, and S3 buckets, catering to a range of data storage formats.

**database_utils.py file:** 
In this file, the DatabaseConnector class is defined to manage database interactions. Its primary purpose is to establish connections to a database and handle the uploading of data, ensuring seamless data integration into the designated storage system.

**data_cleaning.py file:** 
This script introduces the DataCleaning class, equipped with various methods aimed at purifying the data extracted from the aforementioned sources. It focuses on refining the data by removing inconsistencies and standardizing formats, thus preparing it for analysis or storage.

**Note:**
at the end of each of these files, you will see codes in hash. That's because I either didn't want to python to repeat the results again or I was testing the dataframes to get more familiar with what's inside the data.

## License information
