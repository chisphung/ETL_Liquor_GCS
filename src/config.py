from sqlalchemy import create_engine
# Configuration
INPUT_DIR = 'input/'
PROCESSED_DIR = 'processed/'
BATCH_SIZE = 10000
DB_URL = 'sqlite:///liquor_sales.db'

engine = create_engine(DB_URL)

POLL_INTERVAL = 5  # seconds between folder checks

BUCKET_NAME = ' chisphung_etl_process'
INPUT_PATH = 'chisphung_etl_process/chunk_13.csv'
OUTPUT_PATH = 'processed/transformed_data.csv'
PROJECT_ID = 'interns-2025-467409'
DATASET_ID = 'chisphung_liquor_dataset'
TABLE_ID = 'Staging_Sales'