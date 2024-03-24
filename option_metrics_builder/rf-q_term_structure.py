import pandas as pd
import numpy as np
import os


def rf_q(S, K, c, p, t):
    return np.log(K / (S + p - c)) / t


class RQTermStructure():
    def __init__(self, path_data):
        self.path_data = path_data

    def get_processed_data_from_file(self, file_name):
        # Read csv data and keep selected columns
        data = pd.read_csv(os.path.join(self.path_data, file_name), index_col=2)
        raw_columns = [' [EXPIRE_DATE]', ' [STRIKE]', ' [UNDERLYING_LAST]', ' [C_BID]', ' [C_ASK]', ' [P_BID]', ' [P_ASK]']
        data = data[raw_columns]
        # Rename columns
        columns = ['Maturity', 'Strike', 'Spot', 'C_Bid', 'C_Ask', 'P_Bid', 'P_Ask']
        data.columns = columns
        # Casting data
        data.index = pd.to_datetime(data.index)
        data.index.name = 'Date'
        data['Maturity'] = pd.to_datetime(data['Maturity'])
        data.loc[:, 'Strike'] = pd.to_numeric(data['Strike'], errors='coerce')
        data.loc[:, 'S_close'] = pd.to_numeric(data['Spot'], errors='coerce')
        data.loc[:, 'C_Bid'] = pd.to_numeric(data['C_Bid'], errors='coerce')
        data.loc[:, 'C_Ask'] = pd.to_numeric(data['C_Ask'], errors='coerce')
        data.loc[:, 'P_Bid'] = pd.to_numeric(data['P_Bid'], errors='coerce')
        data.loc[:, 'P_Ask'] = pd.to_numeric(data['P_Ask'], errors='coerce')
        # Pre processing
        data['C_Mid'] = data.apply(lambda x: (x['C_Bid'] + x['C_Ask']) / 2, axis=1)
        data['P_Mid'] = data.apply(lambda x: (x['P_Bid'] + x['P_Ask']) / 2, axis=1)
        data['DTM'] = data.apply(lambda x: np.busday_count(np.datetime64(x.name, 'D'), np.datetime64(x['Maturity'], 'D')), axis=1)
        # Drop 0 DTM
        data = data.loc[data['DTM'] != 0]
        # Compute distance from Spot
        data.loc[:, 'Distance'] = data.apply(lambda x: abs(x['Spot'] - x['Strike']), axis=1)
        # Compute rf - q
        data.loc[:, 'rf_q'] = data.apply(lambda x: rf_q(x['Spot'], x['Strike'], x['C_Mid'], x['P_Mid'], x['DTM']), axis=1)
        return data

    def get_term_structure_from_data(self, data):
        # Creating term structure for each observation date
        term_structure = []
        for obs_date in data.index.unique():
            for maturity in data.loc[obs_date, 'Maturity'].unique():
                temp = data[(data.index == obs_date) & (data['Maturity'] == maturity)]
                value = temp[temp['Distance'] == temp['Distance'].min()]['rf_q'].loc[obs_date]
                term_structure.append([obs_date, maturity, value])
        return term_structure

    def get_term_structure(self):
        term_structure = []
        for file_name in os.listdir(self.path_data):
            term_structure.extend(self.get_term_structure_from_data(self.get_processed_data_from_file(file_name)))
        term_structure = pd.DataFrame(term_structure, columns=['Date', 'Maturity', 'rf_q'])
        term_structure.set_index('Date', inplace=True)
        return term_structure


if __name__ == '__main__':
    path_data = r'X:\Main Folder\Options Data\QQQ'
    rfq = RQTermStructure(path_data=os.path.join(path_data, 'Raw Data'))
    term_structure = rfq.get_term_structure()
    term_structure.to_csv(os.path.join(path_data, 'term_structure.csv'))
