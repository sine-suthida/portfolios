import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./webscraping/code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            a = col[1].find_all('a')
            if a[1] is not None:
                data_dict = {
                    'Name' : a[1].contents[0],
                    'MC_USD_Billion' : float(col[2].contents[0].replace("\n",""))
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''    
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    df["MC_GBP_Billion"] = [np.round(x*exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df["MC_EUR_Billion"] = [np.round(x*exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df["MC_INR_Billion"] = [np.round(x*exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(sql_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribute = ['Name', 'MC_USD_Billion']
exchange_path = './webscraping/exchange_rate.csv'
output_path = './webscraping/Largest_banks_data.csv'
log_progress("Preliminaries complete. Initiating ETL process.")

# Call extract() function
extracted_data = extract(url, table_attribute)
log_progress("Data extraction complete. Initiating Transformation process.")

# Call transform() function
transformed_data = transform(extracted_data, exchange_path)
#print(transformed_data['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating loading process.")

# Call load_to_csv() function
load_to_csv(transformed_data, output_path)
log_progress("Data saved to CSV file.")

# Initiate SQLite3 connection
database_name = './webscraping/Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(database_name)
log_progress("SQL Connection initiated")

# Call load_to_db()
load_to_db(transformed_data, conn, table_name)
log_progress("Data loaded to Database as table. Running the query.")

# Call run_query() *
sql_statement = f"SELECT * FROM {table_name}"
run_query(sql_statement, conn)
sql_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(sql_statement, conn)
sql_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(sql_statement, conn)
log_progress("Process Complete.")

# Close SQLite3 connection
conn.close()
log_progress("Server Connection closed")





