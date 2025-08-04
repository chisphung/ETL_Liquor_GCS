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
from src.extract import extract
from src.transform import transform
from src.load import load
import gc
from google.cloud import storage, bigquery
from src.config import INPUT_DIR, PROCESSED_DIR, BATCH_SIZE, engine, POLL_INTERVAL

os.makedirs(PROCESSED_DIR, exist_ok=True)

# class FileHandler(FileSystemEventHandler):
#     def on_created(self, event):
#         if not event.is_directory and event.src_path.endswith('.csv'):
#             print(f"Detected new file: {event.src_path}")
#             process_new_files()

def process_new_files():
    # Step 1: Extract (raw data only)
    has_new_files = extract()
    # exit()
    
    # Steps 2-3: Transform and Load (separated)
    if has_new_files:
        transformed_data = transform()
        load(transformed_data)


if __name__ == "__main__":
    # Initial processing of any existing files
    process_new_files()
    
    # Start continuous processing
    # continuous_processing()