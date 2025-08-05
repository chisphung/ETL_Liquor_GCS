import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import glob
import shutil
import pandas_gbq
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.utils import process_scd_type2, get_processed_files
from google.cloud import bigquery
from src.config import INPUT_DIR, PROCESSED_DIR, BATCH_SIZE, engine, POLL_INTERVAL, PROJECT_ID, DATASET_ID, TABLE_ID

# class Extract:
#     """ Read raw JSON files from Google Storage Bucket """
#     def __init__(self, bucket_files):
#         self.data = []
#         self.bucket_files = bucket_files

def extract(bucket_files = []):
    print("Starting data extraction...")
    processed_files = get_processed_files(project_id=PROJECT_ID)
    # for blob in self.bucket_files:
    #     if blob.name.endswith('.csv'):
    #         # content = blob.download_as_text()
    #         new_files = [blob.download_as_text() if blob.name not in processed_files]
    new_files = [file for file in bucket_files if file.name.endswith('.csv') and file.name not in processed_files]
    print(f"New files to process: {new_files}")
            # df = pd.read_csv(content)
    # csv_files = glob.glob(f"{INPUT_DIR}*.csv")
    # new_files = [f for f in csv_files if os.path.basename(f) not in processed_files]
    downloaded_files = []
    # for blob in bucket_files:
    #     if new_files:
    #         if blob.name in new_files:
    #             blob.download_to_filename(os.path.join(INPUT_DIR, blob.name))
    #             downloaded_files.append(os.path.join(INPUT_DIR, blob.name))

    if not new_files:
        print("No new files to process.")
        return False
    
    print(f"Downloading new files: {[blob.name for blob in new_files]}")
    for blob in new_files:
        file_path = os.path.join(INPUT_DIR, blob.name)
        blob.download_to_filename(file_path)
        downloaded_files.append(file_path)
    
    if not downloaded_files:
        print("No new files to process.")
        return False
                
    for file in downloaded_files:
        print(f"Loading {file} into Staging_Sales...")
        file_basename = os.path.basename(file)
        
        
        for chunk in pd.read_csv(file, chunksize=BATCH_SIZE):
            # Add metadata only - no data processing in extract
            chunk['file_name'] = file_basename
            chunk['processed_timestamp'] = datetime.now()
            
            # Load raw data into staging
            # chunk.to_sql('Staging_Sales', engine, if_exists='append', index=False)
            pandas_gbq.to_gbq(
                chunk,
                f'{DATASET_ID}.Staging_Sales',
                project_id=PROJECT_ID,
                if_exists='append'
            )
            print(f"Loaded chunk from {file} into Staging_Sales.")
        
        # After processing, mark the file as processed
        print(f"Marking {file_basename} as processed.")
        
        # Mark file as processed
        # pd.DataFrame({
        #     'file_name': [file_basename],
        #     'processed_timestamp': [datetime.now()],
        # }).to_sql('Processed_Files', engine, if_exists='append', index=False)
        upload_path = f"{DATASET_ID}.Processed_Files"
        print(f"Uploading processed file metadata to {upload_path}")
        pandas_gbq.to_gbq(
            pd.DataFrame({
                'file_name': [file_basename],
                'processed_timestamp': [datetime.now()],
            }),
            destination_table=upload_path,
            project_id=PROJECT_ID,
            if_exists='append'
        )
        
        # Move file to processed
        shutil.move(file, os.path.join(PROCESSED_DIR, file_basename))
        print(f"Completed loading {file} to staging.")
    
    return True