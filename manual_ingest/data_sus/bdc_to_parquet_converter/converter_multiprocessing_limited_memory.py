import os
import pandas as pd
from dbc_reader import DbcReader
from tqdm import tqdm
from multiprocessing import Pool
from itertools import islice
import time
import psutil

def process_chunk(chunk, file_path):
    # Convert the chunk to a DataFrame
    df = pd.DataFrame(chunk)
    
    # Save DataFrame to Parquet file
    parquet_file_path = os.path.splitext(file_path)[0] + '.parquet'
    df.to_parquet(parquet_file_path)
    
    print(f"Chunk processed and saved as '{parquet_file_path}'")

def process_file(file_name):
    if file_name.endswith('.dbc'):
        # Construct the full file path
        file_path = os.path.join(folder_path, file_name)
        
        # Read the rows from the .dbc file using DbcReader
        reader = DbcReader(file_path)
        
        # Define the chunk size
        chunk_size = 10000  # Adjust the chunk size as per your memory constraints
        
        # Process the file in chunks
        start_time = time.time()
        for chunk in tqdm(iter(lambda: list(islice(reader, chunk_size)), [])):
            process_chunk(chunk, file_path)
        end_time = time.time()
        
        # Get the memory usage
        process = psutil.Process()
        memory_usage = process.memory_info().rss / (1024 ** 2)
        
        # Print the timing, data ingested, and RAM usage
        print(f"File '{file_name}' processed in {end_time - start_time} seconds")
        print(f"{len(chunk)} rows ingested")
        print(f"{memory_usage} MB of RAM used")

if __name__ == '__main__':
    folder_path = '/mnt/ssd/datasus/temp/SIHSUS/'
    
    # Get the list of file names in the folder
    file_names = os.listdir(folder_path)
    
    # Create a Pool object with the desired number of processes (e.g., 3 or 4)
    pool = Pool(processes=3)  # Adjust the number of processes as per your machine's capabilities
    
    # Map the file names to the process_file function using the Pool object
    pool.map(process_file, file_names)
    
    # Close the Pool
    pool.close()
    pool.join()
