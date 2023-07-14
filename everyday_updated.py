# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 16:41:56 2023

@author: taryl
"""

import yfinance as yf
import pandas as pd

# Ticker symbols to retrieve data for
import pyodbc
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from credentials import username, password

# Azure SQL Database connection
server = 'taryllsql.database.windows.net'
database = 'iHub'
driver = '{ODBC Driver 18 for SQL Server}'
connection_string= f'Driver='+driver+';Server='+server+';Database='+database+';Uid='+username+';Pwd='+password+''

#cursor = connection_string.cursor()
# Connect to the SQL Server
connection = pyodbc.connect(connection_string)

# Define the SQL query to read data from the table
sql_select = "SELECT distinct TOP 32 ticker FROM dbo.all_holdings where ticker <> 'None' and ticker is not null "

#use pandas to read data. Execute the query and fetch the existing data code has already been done for you by pandas. you dont need to reinvent the wheel. just focus on business logic.
df_ticker = pd.read_sql(sql_select,connection)
tickers = df_ticker['ticker'].tolist()

today_df = pd.DataFrame(columns=['Date', 'Ticker', 'Price'])


def get_closing_time_value(tickers):
    # Retrieve data from Yahoo Finance
    data = yf.download(tickers, period="1d", interval="1d")

    # Get the closing time and value
    closing_time = data.index[-1].date()
    closing_value = data["Close"].iloc[-1]
    #ticker = data["Tickers"].iloc[-1]
    ticker_df = pd.DataFrame({'Date': closing_time, 'Price': closing_value})
    # Append the ticker's data to the main DataFrame
    #df = df.append(ticker_df)
    ticker_df.reset_index(inplace=True)
    ticker_df.rename(columns={'index': 'Ticker'}, inplace=True)
    ticker_df['Ticker'], ticker_df['Date'] = ticker_df['Date'], ticker_df['Ticker']
    ticker_df = ticker_df.rename(columns={'Ticker': 'Date', 'Date': 'Ticker'})
    #ticker_df = ticker_df.dropna(subset=['Price'])
    ticker_df['Price'] = ticker_df['Price'].astype(object)
    #today_df.append(ticker_df, ignore_index=True)

    print(ticker_df)

    return pd.DataFrame(ticker_df)
#get_closing_time_value(tickers)
today_df = today_df.append(get_closing_time_value(tickers), ignore_index=True)
print(today_df)

table_name = 'all_holdings'  # Replace with your table name

# Fetch all columns from the SQL table
query = f"SELECT TOP 32 * FROM {table_name}"
existing_data = pd.read_sql(query, connection)
#print(existing_data)

merged_df = pd.merge(existing_data, today_df, how='outer')

for col in ['Sector', 'Country', 'Type', 'SecurityName', 'Shares']:
    missing_indices = merged_df[merged_df[col].isna()].index
    for idx in missing_indices:
        match_value = merged_df.loc[merged_df['Ticker'] == merged_df['Ticker'].iloc[idx], col].values
        if len(match_value) > 0:
            merged_df.loc[idx, col] = match_value[0]

#merged_df = merged_df.drop(merged_df.index[:32])
#merged_df = merged_df.iloc[32:]
#merged_df = merged_df.tail(len(merged_df) - 32)
merged_df = merged_df.drop(merged_df.index[:32])
#print(merged_df)
# Create a new DataFrame
updated_today = pd.DataFrame()

# Append the merged_df to the new DataFrame
updated_today = updated_today.append(merged_df)
updated_today.reset_index(drop=True, inplace=True)
print(updated_today)

table_name = 'all_holdings'


#for index, row in updated_today.iterrows():
#     connection.execute("INSERT INTO all_holdings (Date,Ticker,Sector,Country,Type,SecurityName,Shares,Price) values(?,?,?,?,?,?,?,?)", row.Date, row.Ticker, row.Sector, row.Country, row.Type, row.SecurityName, row.Shares, row.Price)
#insert_query = f"INSERT INTO {table_name} (Date, Ticker, Sector, Country, Type, SecurityName, Shares, Price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

 #Insert each row from the DataFrame into the SQL table
#cursor = connection.cursor()
#for row in updated_today.itertuples(index=False):
#    cursor.execute(insert_query, row)
#connection.commit()

#sql_query = "SELECT COUNT(*) FROM dbo.all_holdings"

# Execute the query and fetch the result

#cursor.execute(sql_query)
#row_count = cursor.fetchone()[0]

# Print the number of rows
#print("Number of rows:", row_count)

connection.close()