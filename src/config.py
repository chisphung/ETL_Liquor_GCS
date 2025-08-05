from sqlalchemy import create_engine
from google.cloud import bigquery
# Configuration
INPUT_DIR = 'input/'
PROCESSED_DIR = 'processed/'
BATCH_SIZE = 10000
DB_URL = 'sqlite:///liquor_sales.db'

engine = create_engine(DB_URL)
client = bigquery.Client()

POLL_INTERVAL = 5  # seconds between folder checks

# BUCKET_NAME = 'chris_etl_process'
BUCKET_NAME = 'chisphung_etl_process'
# INPUT_PATH = 'chris_etl_process/chunk_13.csv'
INPUT_PATH = 'chisphung_etl_process/chunk_13.csv'
OUTPUT_PATH = 'processed/transformed_data.csv'
PROJECT_ID = 'interns-2025-467409'
# PROJECT_ID = 'smooth-hub-460704-v8'  
# DATASET_ID = 'chisphung_liquor_dataset'
DATASET_ID = 'chisphung_liquor_dataset'
TABLE_ID = 'Staging_Sales'