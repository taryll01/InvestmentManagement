import pandas as pd
import yfinance as yf
import datetime
from pandas import DataFrame


def get_historical_data(tickers, start_date, end_date) -> DataFrame:
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
                historical_data["Ticker"] = ticker
                dataframes.append(historical_data)

        except Exception as e:
            error_message = str(e)
            if (
                "404 Client Error" in error_message
                or "symbol may be delisted" in error_message
            ):
                tickers_with_no_data.append(ticker)
            else:
                print(f"An error occurred for symbol {ticker}:", error_message)
            continue

    df_all_historical_data = pd.concat(dataframes)
    df_all_historical_data = df_all_historical_data.reset_index()

    print("No data for ", tickers_with_no_data)
    return df_all_historical_data


def fetch_latest_data(tickers: list, sec_id: list) -> pd.DataFrame:
    dataframes = []

    if len(tickers) != len(sec_id):
        raise ValueError("Length of tickers and sec_id lists must be the same.")

    for ticker, sec in zip(tickers, sec_id):
        try:
            stock = yf.Ticker(ticker)
            latest_data = stock.history(period="1d")
            
            # To get dataframe in the ORM DB form to bulk insert to DB
            if not latest_data.empty: 
                latest_data.insert(0, "AsOfDate", latest_data.index.date) 
                latest_data.insert(1, "SecID", sec) 
                latest_data.rename(columns={"Stock Splits": "Stock_Splits"}, inplace=True)
                dataframes.append(latest_data)
                
        except Exception as e:
            error_message = str(e)
            if (
                "404 Client Error" in error_message
                or "symbol may be delisted" in error_message
            ):
                print(f"No data found for symbol {ticker}.")
            else:
                print(f"An error occurred for symbol {ticker}:", error_message)
            continue

    df_latest_data = pd.concat(dataframes)
    df_latest_data["Dataload_Date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_latest_data = df_latest_data.reset_index(drop=True)

    return df_latest_data
