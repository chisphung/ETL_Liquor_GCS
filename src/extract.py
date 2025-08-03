import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import glob
import shutil
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.utils import process_scd_type2, get_processed_files
import gc
from src.config import INPUT_DIR, PROCESSED_DIR, BATCH_SIZE, engine, POLL_INTERVAL


def extract():
    processed_files = get_processed_files(engine)
    csv_files = glob.glob(f"{INPUT_DIR}*.csv")
    new_files = [f for f in csv_files if os.path.basename(f) not in processed_files]
    
    if not new_files:
        print("No new files to process.")
        return False
    
    for file in new_files:
        print(f"Loading {file} into Staging_Sales...")
        file_basename = os.path.basename(file)
        
        
        for chunk in pd.read_csv(file, chunksize=BATCH_SIZE):
            # Add metadata only - no data processing in extract
            chunk['file_name'] = file_basename
            chunk['processed_timestamp'] = datetime.now()
            
            # Load raw data into staging
            chunk.to_sql('Staging_Sales', engine, if_exists='append', index=False)
        
        # Mark file as processed
        pd.DataFrame({
            'file_name': [file_basename],
            'processed_timestamp': [datetime.now()],
        }).to_sql('Processed_Files', engine, if_exists='append', index=False)
        
        # Move file to processed
        shutil.move(file, os.path.join(PROCESSED_DIR, file_basename))
        print(f"Completed loading {file} to staging.")
    
    return True