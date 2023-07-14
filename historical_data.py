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
sql_select = "SELECT distinct ticker FROM dbo.all_holdings where ticker <> 'None' and ticker is not null "

#use pandas to read data. Execute the query and fetch the existing data code has already been done for you by pandas. you dont need to reinvent the wheel. just focus on business logic.
df_ticker = pd.read_sql(sql_select,connection)
tickers = df_ticker['ticker'].tolist()

# Add new data for today and update the table for each ticker
today = datetime.now().date()

end_date = datetime.now().date() - timedelta(days=1)
start_date = end_date - timedelta(days=365)


tickers_with_no_data = []
dataframes = []

for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        stock.info
        historical_data = stock.history(start=start_date, end=end_date)
        if historical_data.empty:
            tickers_with_no_data.append(ticker)
        else:
            historical_data['ticker'] = ticker
            dataframes.append(historical_data)
            df_symbol = df_ticker[df_ticker['Ticker'] == ticker].copy() ####symbol replaced by ticker

            data = data[['Close']].reset_index()

            # Rename the columns to match the SQL table columns
            data.rename(columns={'Date': 'Date', 'Close': 'Price'}, inplace=True)

            # Filter the existing data for the current symbol
            

            # Set the other columns from the existing data
            data['Sector'] = df_symbol['Sector'].iloc[0]
            data['Country'] = df_symbol['Country'].iloc[0]
            data['Type'] = df_symbol['Type'].iloc[0]
            data['SecurityName'] = df_symbol['SecurityName'].iloc[0]
            data['Shares'] = df_symbol['Shares'].iloc[0]

           

            print(f"Data inserted for symbol: {ticker}")
            
    except Exception as e:
        error_message = str(e)
        if "404 Client Error" in str(e) or "symbol may be delisted" in str(e):
            tickers_with_no_data.append(ticker)
        else:
            print(f"An error occurred for symbol {ticker}:", str(e))
        continue

df_all_historical_data =  pd.concat(dataframes)
df_all_historical_data = df_all_historical_data.reset_index()
#print(df_all_historical_data)
print(tickers_with_no_data)

columns_to_insert = ['Date', 'ticker', 'Close']
#print(df_all_historical_data[columns_to_insert])

data_365 = df_all_historical_data[columns_to_insert]
data_365 = data_365.rename(columns={'ticker': 'Ticker', 'Close':'Price'})
data_365['Date'] = pd.to_datetime(data_365['Date'])
# Extract the date portion using the .dt.date accessor
data_365['Date'] = data_365['Date'].dt.date
data_365['Price'] = data_365['Price'].astype(object)
print(data_365)


table_name = 'all_holdings'  # Replace with your table name

# Fetch all columns from the SQL table
query = f"SELECT * FROM {table_name}"
existing_data = pd.read_sql(query, connection)
print(existing_data)



merged_df = pd.merge(existing_data, data_365, how='outer')

for col in ['Sector', 'Country', 'Type', 'SecurityName', 'Shares']:
    missing_indices = merged_df[merged_df[col].isna()].index
    for idx in missing_indices:
        match_value = merged_df.loc[merged_df['Ticker'] == merged_df['Ticker'].iloc[idx], col].values
        if len(match_value) > 0:
            merged_df.loc[idx, col] = match_value[0]

print(merged_df)
#merged_df.to_csv('data.txt', sep='\t', index=False)




#for index, row in merged_df.iterrows():
#     connection.execute("INSERT INTO all_holdings (Date,Ticker,Sector,Country,Type,SecurityName,Shares,Price) values(?,?,?,?,?,?,?,?)", row.Date, row.Ticker, row.Sector, row.Country, row.Type, row.SecurityName, row.Shares, row.Price)
#insert_query = f"INSERT INTO {table_name} (Date, Ticker, Sector, Country, Type, SecurityName, Shares, Price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

# Insert each row from the DataFrame into the SQL table
#cursor = connection.cursor()
#for row in merged_df.itertuples(index=False):
#    cursor.execute(insert_query, row)
#connection.commit()

connection.close()
