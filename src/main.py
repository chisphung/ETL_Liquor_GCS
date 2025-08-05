from src.extract import extract
from src.transform import transform
from src.load import load
from src.config import BUCKET_NAME, DATASET_ID, TABLE_ID, INPUT_PATH, OUTPUT_PATH
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
        if not extractor:
            print("No new files to process. Exiting pipeline.")
            return
        transformer = transform()

        load(transformed_data=transformer, dataset_name=DATASET_ID)


if __name__ == "__main__":


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