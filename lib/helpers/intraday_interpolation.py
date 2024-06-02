import os
import pandas as pd
import numpy as np


def convert_to_ny_timezone(row):
    # Combine date and time into a single string
    datetime_str = f"{row['<DTYYYYMMDD>']} {row['<TIME>']}"

    # Convert string to datetime object in GMT timezone
    datetime_gmt = pd.to_datetime(datetime_str, format='%Y%m%d %H%M').tz_localize('GMT')

    # Convert to New York timezone
    datetime_ny = datetime_gmt.tz_convert('America/New_York')

    return datetime_ny


# Define a function to convert the timestamp
def convert_timezone(ts):
    # Convert to America/New_York timezone
    ts = ts.tz_convert('America/New_York')
    # Reset the time to 09:30
    ts = ts.replace(hour=16, minute=00)
    return ts

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    file_path_eod = r"X:\Main Folder\Data\Spot\QQQ Daily.csv"
    data_eod = pd.read_csv(file_path_eod, index_col=0, parse_dates=True)
    data_eod.index = data_eod.index.map(convert_timezone)
    data_eod['Previous Close'] = data_eod['Close'].shift(1)


    file_path_intraday = r"X:\Main Folder\Data\Spot\USA100_15min.csv"
    data = pd.read_csv(file_path_intraday)
    data = data[data['<TIME>'].apply(str).apply(len) == 4]
    data['Date'] = data.apply(convert_to_ny_timezone, axis=1)
    data.set_index('Date', inplace=True)
    data = data[['<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>']]
    data.columns = ['Open', 'High', 'Low', 'Close']

    data = data.between_time('09:30', '16:00')

    data['open_day'] = data.apply(lambda x: data['Open'].asof(x.name.replace(hour=9, minute=30)), axis=1)
    data['close_day'] = data.apply(lambda x: data['Close'].asof(x.name.replace(hour=16, minute=00)), axis=1)
    data['previous_close_day'] = data.apply(lambda x: data['Close'].asof(x.name.replace(hour=9, minute=00)), axis=1)

    data['open_eod'] = data.apply(lambda x: np.nan if x.name.replace(hour=16, minute=00) not in data_eod.index else data_eod.at[x.name.replace(hour=16, minute=0), 'Open'], axis=1)
    data['close_eod'] = data.apply(lambda x: np.nan if x.name.replace(hour=16, minute=00) not in data_eod.index else data_eod.at[x.name.replace(hour=16, minute=0), 'Close'], axis=1)
    data['previous_close_eod'] = data.apply(lambda x: np.nan if x.name.replace(hour=16, minute=00) not in data_eod.index else data_eod.at[x.name.replace(hour=16, minute=0), 'Previous Close'], axis=1)
    data.dropna(inplace=True)


    data_new = pd.DataFrame(index=data.index, columns=['Open', 'High', 'Low', 'Close'])

    for column in ['Open', 'High', 'Low', 'Close']:
        data_new.loc[:, column] = data.apply(lambda x: (x[column] - x['previous_close_day']) / (x['close_day'] - x['previous_close_day']) * (x['close_eod'] - x['previous_close_eod']) + x['previous_close_eod'], axis=1)

    data_new.dropna(inplace=True)

    data_new.to_csv(r"X:\Main Folder\Data\Spot\QQQ Intraday.csv")

    print('end')
