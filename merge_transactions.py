import pandas as pd
import os
import glob
import pyarrow
import fastparquet
# get all directory path
base_dir = "/home/1250"  # replace with your top-level directory path

# get all transactions_* directory
top_level_directories = glob.glob(os.path.join(base_dir, "transactions_*"))

# create empty DataFrame to store mereged data
merged_df = pd.DataFrame()

# iterate each directory
for top_dir in top_level_directories:
    # fetch .parquet file in the directories
    parquet_files = glob.glob(os.path.join(top_dir, "**/*.parquet"), recursive=True)
    
    # iterate each file and read the data
    for file in parquet_files:
        # Extract the date in the file path（format：order_datetime=YYYY-MM-DD）
        # initially find order_datetime=YYYY-MM-DD，then extract the date
        parts = file.split(os.sep)  # Split paths according to the system's path separator
        date_part = None
        for part in parts:
            if part.startswith("order_datetime="):
                date_part = part.replace("order_datetime=", "")
                break
        
        if date_part is not None:
            # read parquet file
            df = pd.read_parquet(file)
            
            # add 'date' column
            df['date'] = pd.to_datetime(date_part)
            
            # Add the data from the current file to the merge to DataFrame
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        print(f"{file} is done")

# save the final merged data
merged_df.to_csv("/home/1250/merged_transactions.csv", index=False)

print(f"merge completed, total {len(merged_df)} data")
