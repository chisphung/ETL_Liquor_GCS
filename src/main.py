""" Main.py file to execute Extract, Transform, Load Job"""

from src.common import (
    CloudStorage,
    UserCredentials,
    BucketConfig
)

from src.extract import extract
from src.transform import transform
from src.load import load
from src.config import PROJECT_ID, BUCKET_NAME, DATASET_ID, TABLE_ID, INPUT_PATH, OUTPUT_PATH
from google.cloud import storage
# from src.load import Load
class ELTPipeline:
    """ Run Extract → Transform → Load """
    def __init__(self, bucket_name, creds, bucket_files):
        self.bucket_name = bucket_name
        self.storage_client = creds
        self.bucket_files = bucket_files

    def run(self):
        """ Method to execute ETL Pipeline"""
        extractor = extract(bucket_files=self.bucket_files)

        transformer = transform()

        load(transformed_data=transformer, dataset_name=DATASET_ID)


if __name__ == "__main__":
    # CREDS = UserCredentials().get_credentials()

    # cloud_storage = CloudStorage(credentials=CREDS)

    # Uncomment the below code to upload local dataset to Google Cloud Storage
    # cloud_storage.insert_blob(bucket_name=BucketConfig.BUCKET_NAME, local_dir='raw-data/')

    storage_client = storage.Client()

    BUCKET_FILES = storage_client.get_bucket(BUCKET_NAME)
    print(type(BUCKET_FILES))
    print(f"Files in bucket {BUCKET_NAME}: {[file.name for file in BUCKET_FILES.list_blobs()]}")
    pipeline = ELTPipeline(
        bucket_name=BUCKET_NAME,
        creds=storage_client,
        bucket_files=BUCKET_FILES.list_blobs()
    )
    pipeline.run()