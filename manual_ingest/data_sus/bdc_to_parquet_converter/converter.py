import os
import pandas as pd
from dbc_reader import DbcReader

folder_path = '/home/chic/repos/data_sus_tik/fct-unesp-datasus/landing/SIHSUS'

# Iterate through each file in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.dbc'):
        # Construct the full file path
        file_path = os.path.join(folder_path, file_name)
        
        # Read the rows from the .dbc file using DbcReader
        rows = [row for row in DbcReader(file_path)]
        
        # Create a DataFrame from the rows
        df = pd.DataFrame(rows)
        
        # Save DataFrame to Parquet file
        parquet_file_path = os.path.splitext(file_path)[0] + '.parquet'
        df.to_parquet(parquet_file_path)
        
        print(f"File '{file_name}' converted to DataFrame and saved as '{parquet_file_path}'")
