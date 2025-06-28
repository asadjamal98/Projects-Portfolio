import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import logging



def setup_logger():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s : %(message)s")

def extract(url, table_attribs):

    # Getting webpage
    webpage = requests.get(url).text

    # Parsing webpage and saving table from webpage 
    data = BeautifulSoup(webpage,'html.parser')
    tbl = data.find('table', class_='wikitable')

    # Getting required data into a data frame 
    tbl_data = tbl.find_all('tr')
    list_of_data = []

    for row in tbl_data[2:]:    
        tbl_row = [data.text.strip() for data in row.find_all('td')]
        country = tbl_row[0]
        if tbl_row[2] != '—':
            gdp = tbl_row[2]
            year = tbl_row[3][-4:]
        else:
            gdp = None
            year = None
        list_of_data.append([country, gdp, year])

    df = pd.DataFrame(list_of_data, columns = table_attribs)
    return df

def transform(df):
    col_name = df.columns[2]
    df[col_name] = (df[col_name].str.replace(',', '').astype(float) / 1000).round(2)
    return df

def load_to_json(df, json_path):
    df.to_json(json_path)

def load_to_database(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    return query_output


# Set required variable to run script
URL = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29" # URL of webpage
json_path = './Countries_by_GDP.json' # Path to json file
connection_to_db = sqlite3.connect('World_Economies.db') # Database name and connection setup
table_name = 'Countries_by_GDP' # Name of table to be created in database
table_attribs = ['Country', 'GDP_USD_Billion', 'Year'] # Table attributes
test_query = f"SELECT * FROM {table_name} WHERE {table_attribs[1]} >= 100" # Query for testing out database

def main():
    
    setup_logger()
    
    logging.info("Preliminaries complete.Initiating ETL process...")
    logging.info("Initiating ETL process...")
    extracted_df = extract(URL, table_attribs)
    
    logging.info("Data extraction complete.")
    logging.info("Initiating Transformation process...")
    transformed_df = transform(extracted_df)
    
    logging.info('Data transformation complete.')
    logging.info("Initiating loading process...")
    load_to_json(transformed_df, json_path)

    logging.info('Data saved to CSV file')
    logging.info("Loading data to database...")
    load_to_database(transformed_df, connection_to_db, table_name)

    logging.info('Data loaded to Database as table.')
    logging.info("Running the test query: %s", test_query)
    logging.info("Query result: \n%s", run_query(test_query, connection_to_db))

    logging.info('Process Complete.')
    connection_to_db.close()

if __name__=="__main__":
    main()






