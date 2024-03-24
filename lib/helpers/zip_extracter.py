import os
import py7zr


def extract_7z_files(path_zip, path_target):
    # Create the target directory if it doesn't exist
    if not os.path.exists(path_target):
        os.makedirs(path_target)

    # List all files in the zip folder
    files = [f for f in os.listdir(path_zip) if f.endswith('.7z')]

    # Extract each .7z file
    for file in files:
        zip_file = os.path.join(path_zip, file)
        with py7zr.SevenZipFile(zip_file, mode='r') as zip_ref:
            zip_ref.extractall(path_target)


def txt_to_csv(path_folder):
    # List all files in the folder
    files = [f for f in os.listdir(path_folder) if os.path.isfile(os.path.join(path_folder, f))]

    # Iterate through each file
    for file_name in files:
        if file_name.endswith('.txt'):
            # Construct the paths
            old_path = os.path.join(path_folder, file_name)
            new_path = os.path.join(path_folder, os.path.splitext(file_name)[0] + '.csv')

            # Read the content of the txt file
            with open(old_path, 'r') as file:
                content = file.read()

            # Write the content to a new file with csv extension
            with open(new_path, 'w') as file:
                file.write(content)

            # Optionally, you can remove the original .txt file
            os.remove(old_path)


path_zip = r'X:\Main Folder\Options Data\QQQ\Zip'
path_target = r'X:\Main Folder\Options Data\QQQ\Raw'

extract_7z_files(path_zip, path_target)

txt_to_csv(path_target)