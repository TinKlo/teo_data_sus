import os
import pandas as pd
from dbc_reader import DbcReader
from multiprocessing import Pool
import resource

def process_file(file_name):
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

if __name__ == '__main__':
    folder_path = '/home/chic/repos/data_sus_tik/fct-unesp-datasus/landing/SIHSUS'
    max_memory = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 0.85)  # 85% of the maximum RAM usage
    
    # Set the maximum memory limit for each worker process
    resource.setrlimit(resource.RLIMIT_AS, (max_memory, max_memory))
    
    # Get the list of file names in the folder
    file_names = os.listdir(folder_path)
    
    # Create a Pool object with the desired number of processes (e.g., 3 or 4)
    pool = Pool(processes=3)  # Adjust the number of processes as per your machine's capabilities
    
    # Map the file names to the process_file function using the Pool object
    pool.map(process_file, file_names)
    
    # Close the Pool
    pool.close()
    pool.join()
