import os
from google.cloud import storage
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

class UserCredentials:
    """Create credentials for user authentication using OAuth 2.0."""

    def __init__(self, client_secrets_path='client_secrets.json', token_path='token.json'):
        self.__client_secrets_path = client_secrets_path
        self.__token_path = token_path
        self.__scopes = ['https://www.googleapis.com/auth/cloud-platform']
        self.__credentials = self.__create_connection()

    def __create_connection(self):
        """Create connection using OAuth 2.0 user credentials."""
        creds = None
        # Check if token exists and is valid
        if os.path.exists(self.__token_path):
            creds = Credentials.from_authorized_user_file(self.__token_path, self.__scopes)
        
        # If no valid credentials, prompt user for authentication
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.__client_secrets_path, self.__scopes)
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for future use
            with open(self.__token_path, 'w') as token:
                token.write(creds.to_json())
        
        return creds

    def get_credentials(self):
        """Return the credentials object."""
        return self.__credentials

class BucketConfig:
    """Bucket configuration."""
    BUCKET_NAME = 'sj-json-bucket'
    LOCAL_JSON_FILE = 'raw-data/'

class CloudStorage:
    """CloudStorage helper class to perform creation and deletion of buckets."""
    def __init__(self, credentials):
        # Initialize GCS client with user credentials
        self.storage_client = storage.Client(credentials=credentials)

    def create_bucket(self, bucket_name):
        """Create a GCS Bucket if it doesn't exist."""
        bucket = self.storage_client.bucket(bucket_name=bucket_name)
        if not bucket.exists():
            bucket = self.storage_client.create_bucket(
                bucket_or_name=bucket_name,
                location="asia-south1"
            )
            print(f"Bucket {bucket_name} created.")
        else:
            print(f"Bucket {bucket_name} already exists.")
        return bucket

    def delete_bucket(self, bucket_name, force=False):
        """Delete a GCS bucket. Set force=True to delete even if not empty."""
        bucket = self.storage_client.bucket(bucket_name)
        if force:
            blobs = list(bucket.list_blobs())
            for blob in blobs:
                blob.delete()
                print(f"Deleted blob: {blob.name}")
        bucket.delete()
        print(f"Bucket {bucket_name} deleted.")

    def read_bucket(self, bucket_name, prefix=None):
        """Read contents from the bucket_name."""
        bucket = self.storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return blobs

    def insert_blob(self, bucket_name: str, local_dir: str):
        """
        Upload multiple JSON files from a local directory to a GCS bucket.

        Args:
            bucket_name (str): Name of the target bucket.
            local_dir (str): Local directory containing JSON files.
        """
        bucket = self.storage_client.bucket(bucket_name)
        if not os.path.exists(local_dir):
            print(f"Local directory '{local_dir}' not found.")
            return
        files_uploaded = 0
        for file_name in os.listdir(local_dir):
            if file_name.endswith('.json'):
                local_path = os.path.join(local_dir, file_name)
                blob = bucket.blob(file_name)
                blob.upload_from_filename(local_path)
                print(f"Uploaded: {file_name} → gs://{bucket_name}/{file_name}")
                files_uploaded += 1

        if files_uploaded == 0:
            print("No JSON files found to upload.")
        else:
            print(f"Uploaded {files_uploaded} file(s) successfully.")