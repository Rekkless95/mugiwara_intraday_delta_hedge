import os
import pandas as pd


def pre_process(file_name):
    # Read csv data and keep selected columns
    raw_data = pd.read_csv(file_name)

    call_raw_columns = [' [QUOTE_DATE]', ' [EXPIRE_DATE]', ' [STRIKE]', ' [C_BID]', ' [C_ASK]', ' [C_VOLUME]', ' [C_IV]', ' [C_DELTA]', ' [C_GAMMA]', ' [C_VEGA]', ' [C_THETA]', ' [C_RHO]']
    call_data = raw_data.copy()
    call_data = call_data[call_raw_columns]
    # Rename columns
    columns = ['Date', 'Maturity', 'Strike', 'Bid', 'Ask', 'Volume', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']
    call_data.columns = columns
    # Casting call_data
    call_data['Date'] = pd.to_datetime(call_data['Date'])
    call_data['Maturity'] = pd.to_datetime(call_data['Maturity'])
    call_data.loc[:, 'Strike'] = pd.to_numeric(call_data['Strike'], errors='coerce')
    call_data.loc[:, 'Bid'] = pd.to_numeric(call_data['Bid'], errors='coerce')
    call_data.loc[:, 'Ask'] = pd.to_numeric(call_data['Ask'], errors='coerce')
    call_data.loc[:, 'Volume'] = pd.to_numeric(call_data['Volume'], errors='coerce')
    call_data.loc[:, 'Implied Vol'] = pd.to_numeric(call_data['Implied Vol'], errors='coerce')
    call_data.loc[:, 'Delta'] = pd.to_numeric(call_data['Delta'], errors='coerce')
    call_data.loc[:, 'Gamma'] = pd.to_numeric(call_data['Gamma'], errors='coerce')
    call_data.loc[:, 'Vega'] = pd.to_numeric(call_data['Vega'], errors='coerce')
    call_data.loc[:, 'Theta'] = pd.to_numeric(call_data['Theta'], errors='coerce')
    call_data.loc[:, 'Rho'] = pd.to_numeric(call_data['Rho'], errors='coerce')
    call_data['Type'] = [1] * len(call_data)

    call_raw_columns = [' [QUOTE_DATE]', ' [EXPIRE_DATE]', ' [STRIKE]', ' [P_BID]', ' [P_ASK]', ' [P_VOLUME]', ' [P_IV]', ' [P_DELTA]', ' [P_GAMMA]', ' [P_VEGA]', ' [P_THETA]', ' [P_RHO]']
    put_data = raw_data.copy()
    put_data = put_data[call_raw_columns]
    # Rename columns
    columns = ['Date', 'Maturity', 'Strike', 'Bid', 'Ask', 'Volume', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']
    put_data.columns = columns
    # Casting put_data
    put_data['Date'] = pd.to_datetime(put_data['Date'])
    put_data['Maturity'] = pd.to_datetime(put_data['Maturity'])
    put_data.loc[:, 'Strike'] = pd.to_numeric(put_data['Strike'], errors='coerce')
    put_data.loc[:, 'Bid'] = pd.to_numeric(put_data['Bid'], errors='coerce')
    put_data.loc[:, 'Ask'] = pd.to_numeric(put_data['Ask'], errors='coerce')
    put_data.loc[:, 'Volume'] = pd.to_numeric(put_data['Volume'], errors='coerce')
    put_data.loc[:, 'Implied Vol'] = pd.to_numeric(put_data['Implied Vol'], errors='coerce')
    put_data.loc[:, 'Delta'] = pd.to_numeric(put_data['Delta'], errors='coerce')
    put_data.loc[:, 'Gamma'] = pd.to_numeric(put_data['Gamma'], errors='coerce')
    put_data.loc[:, 'Vega'] = pd.to_numeric(put_data['Vega'], errors='coerce')
    put_data.loc[:, 'Theta'] = pd.to_numeric(put_data['Theta'], errors='coerce')
    put_data.loc[:, 'Rho'] = pd.to_numeric(put_data['Rho'], errors='coerce')
    put_data['Type'] = [0] * len(put_data)

    data = pd.concat([call_data, put_data])

    return data


def process_files(folder_path, target_path):
    # Create the target directory if it doesn't exist
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    # List all files in the folder
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    # Iterate through each file
    for file_name in files:
        # Construct the file paths
        input_file_path = os.path.join(folder_path, file_name)
        output_file_path = os.path.join(target_path, file_name)

        # Apply pre_process function to each file
        df = pre_process(input_file_path)

        # Save the resulting DataFrame as a CSV file
        df.to_csv(output_file_path, index=False)
        print(f"Processed file saved: {output_file_path}")


# Example usage:
folder_path = r'X:\Main Folder\Options Data\QQQ\Raw'
target_path = r'X:\Main Folder\Options Data\QQQ\Pre Processed'

process_files(folder_path, target_path)