import os
import requests
import zipfile
import pandas as pd

# Step 1: Download the Excel file from the ABS website
def download_excel(url, download_path):
    # Check if the directory exists, if not create it
    dir_name = os.path.dirname(download_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        print(f"Directory {dir_name} created.")
    
    response = requests.get(url)
    if response.status_code == 200:
        with open(download_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved at: {download_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

# Step 2: Download and unzip the file from the second URL
def download_and_unzip(zip_url, download_zip_path, extract_to_path):
    # Check if the directory exists, if not create it
    dir_name = os.path.dirname(download_zip_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
        print(f"Directory {dir_name} created.")
    
    response = requests.get(zip_url)
    if response.status_code == 200:
        with open(download_zip_path, 'wb') as file:
            file.write(response.content)
        print(f"ZIP file downloaded successfully and saved at: {download_zip_path}")
        
        # Unzipping the file
        with zipfile.ZipFile(download_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)
            print(f"ZIP file extracted to: {extract_to_path}")
    else:
        print(f"Failed to download the ZIP file. Status code: {response.status_code}")

# Step 3: Transform - Read the specific sheet (Table 3) from the Excel file
def read_excel_file(file_path, sheet_name='Table 3'):
    if os.path.exists(file_path):
        try:
            data = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"Excel file loaded successfully from: {file_path}, sheet: {sheet_name}")
            return data
        except ValueError:
            print(f"Error: Sheet '{sheet_name}' not found in the Excel file.")
            return None
    else:
        print(f"File does not exist: {file_path}")
        return None

# Step 4: Load - Save the processed sheet to a CSV file
def save_to_csv(df, output_csv_path):
    if df is not None:
        df.to_csv(output_csv_path, index=False)
        print(f"Data saved successfully to: {output_csv_path}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    # URL for the Excel file
    excel_url = "https://www.abs.gov.au/statistics/labour/employment-and-unemployment/labour-force-australia-detailed/jul-2024/MRM1.xlsx"
    download_path = "data/external/MRM1.xlsx"  # Save file to the specified directory
    
    # URL for the first ZIP file
    zip_url1 = "https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055006_cg_postcode_2011_sa4_2011.zip&1270.0.55.006&Data%20Cubes&86D6D239B79547A2CA257A4B0014E9A0&0&July%202011&31.07.2012&Latest"
    download_zip_path1 = "data/external/sa4_postcode.zip"  # Path to save the first ZIP file
    extract_to_path1 = "data/external/"  # Path to extract the first ZIP file

    # URL for the second ZIP file (shapefile)
    zip_url2 = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA4_2021_AUST_SHP_GDA2020.zip"
    download_zip_path2 = "data/external/SA4_2021_AUST_SHP_GDA2020.zip"  # Path to save the second ZIP file
    extract_to_path2 = "data/external"  # Path to extract the second ZIP file

    # Step 1: Download the Excel file
    download_excel(excel_url, download_path)

    # Step 2: Read the third sheet ('Table 2') from the downloaded Excel file
    df = read_excel_file(download_path, sheet_name='Table 2')

    # Step 3: Save the 'Table 2' sheet to CSV
    output_csv_path = "data/external/Unemployment_Rate_SA4.csv"
    save_to_csv(df, output_csv_path)

    # Step 4: Download and unzip the first ZIP file
    download_and_unzip(zip_url1, download_zip_path1, extract_to_path1)

    # Step 5: After unzipping, extract 'Table 3' from the unzipped Excel file and save it as CSV
    unzipped_excel_path = os.path.join(extract_to_path1, '1270055006_CG_POSTCODE_2011_SA4_2011.xls')  # Change the file name if necessary
    table3_df = read_excel_file(unzipped_excel_path, sheet_name='Table 3')
    
    # Save 'Table 3' to CSV
    output_csv_table3 = "data/external/POSTCODE_2011_TO_SA4.csv"
    save_to_csv(table3_df, output_csv_table3)

    # Step 6: Download and unzip the second ZIP file (shapefile)
    download_and_unzip(zip_url2, download_zip_path2, extract_to_path2)

    # No need to further process shapefiles, just make sure they're unzipped
    print(f"Shapefiles extracted to {extract_to_path2}")
