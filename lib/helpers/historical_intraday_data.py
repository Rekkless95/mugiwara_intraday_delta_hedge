import requests
import pandas as pd
import os
import yfinance as yf


def get_intraday_data(symbol, api_key, start_date, end_date, frequency):
    """
    Get intraday OHLC and volume data for a symbol from Alpha Vantage.

    Parameters:
        symbol (str): The symbol of the security (e.g., 'QQQ').
        api_key (str): Your Alpha Vantage API key.
        start_date (str): Start date in the format 'YYYY-MM-DD'.
        end_date (str): End date in the format 'YYYY-MM-DD'.
        frequency (str): Data frequency (e.g., '1min', '5min', '15min', '30min', '60min').

    Returns:
        pd.DataFrame: A DataFrame containing OHLC and volume data.
    """
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={frequency}&apikey={api_key}&outputsize=full&datatype=csv'
    response = requests.get(url)
    data = pd.read_csv(url)

    # Convert date column to datetime and set as index
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data.set_index('timestamp', inplace=True)

    # Sort DataFrame by DateTimeIndex in ascending order
    data.sort_index(inplace=True)

    # Filter data for the specified date range
    data = data.loc[start_date:end_date]

    return data


def get_intraday_data(symbol, start_date, end_date, interval):
    """
    Get 1-hour intraday OHLC data for a symbol from Yahoo Finance.

    Parameters:
        symbol (str): The symbol of the security (e.g., 'QQQ').
        start_date (str): Start date in the format 'YYYY-MM-DD'.
        end_date (str): End date in the format 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: A DataFrame containing OHLC data.
    """
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date, end=end_date, interval=interval)

    return data

# Define symbol (QQQ), start date, and end date
symbol = '^VXN'
start_date = '2010-01-01'
end_date = '2023-12-31'
interval = '1d'

dir_path = r'X:\Main Folder\Options Data'
dir_path = os.path.join(dir_path, symbol, 'spot')

dir_path = r'X:\Main Folder\Options Data\QQQ\spot'


# Get 1-hour intraday data
intraday_data = get_intraday_data(symbol, start_date, end_date, interval)
print("Successfully retrieved intraday data:")
print(intraday_data)
# Save the data to a CSV file
file_name = f'{symbol}_intraday_{interval}.csv'
file_path = os.path.join(dir_path, file_name)
intraday_data.to_csv(file_path)
print(f"Intraday data saved to {symbol}_intraday_{interval}.csv")


# # Set your Alpha Vantage API key
# api_key = 'MZZGM21KNW4PYVEG'
#
# # Define symbol (QQQ), start date, end date, and frequency
# symbol = 'QQQ'
# start_date = '2010-01-01'
# end_date = '2023-12-31'  # Assuming you want data for one day
# frequency = 'daily'
# dir_path = r'X:\Main Folder\Options Data'
# dir_path = os.path.join(dir_path, symbol, 'spot')
#
# # Get intraday data
# try:
#     intraday_data = get_intraday_data(symbol, api_key, start_date, end_date, frequency)
#     print("Successfully retrieved intraday data:")
#     print(intraday_data)
#     # Save the data to a CSV file
#     file_name = f'{symbol}_intraday_{frequency}.csv'
#     file_path = os.path.join(dir_path, file_name)
#     intraday_data.to_csv(file_path)
#     print(f"Intraday data saved to {symbol}_intraday_{frequency}.csv")
# except Exception as e:
#     print("Error:", e)

