import os
import pandas as pd


def aggregate_index_values(data_path, target_path):
    # List all files in the folder
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

    # Initialize a set to store unique index values
    unique_values = set()

    # Iterate through each file
    for file_name in files:
        file_path = os.path.join(data_path, file_name)

        # Read the CSV file, considering the index column (index_col=2)
        df = pd.read_csv(file_path, index_col=2)

        # Extract unique index values and add them to the set
        unique_values.update(df.index.unique())

    # Sort the unique index values
    sorted_values = sorted(unique_values)

    # Write the sorted values to a new CSV file
    target_file_path = os.path.join(target_path, 'aggregated_values.csv')
    pd.DataFrame(sorted_values, columns=['Index']).to_csv(target_file_path, index=False)


# Example usage:
data_path = r'X:\Main Folder\Options Data\QQQ\Raw'
target_path = r'X:\Main Folder\Options Data\QQQ'

aggregate_index_values(data_path, target_path)