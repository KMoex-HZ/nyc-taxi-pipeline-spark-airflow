import requests
import os
from minio import Minio
from io import BytesIO

# --- MinIO Object Storage Configuration ---
# Configured for Docker-internal networking using the 'minio' service alias
MINIO_URL = "minio:9000" 
ACCESS_KEY = "admin"
SECRET_KEY = "password123"
BUCKET_NAME = "nyc-taxi"

# --- Initialize MinIO Client ---
client = Minio(
    MINIO_URL,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

def download_and_upload():
    """
    Orchestrates the Extraction and Loading (EL) process:
    1. Ensures the target landing zone (MinIO Bucket) exists.
    2. Extracts raw Parquet data from the NYC Taxi TLC public dataset.
    3. Streams the data into MinIO for downstream Spark processing.
    """
    # 1. Landing Zone Verification (Bucket Initialization)
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"📁 Initialized new bucket: '{BUCKET_NAME}'")
        else:
            print(f"📁 Target bucket '{BUCKET_NAME}' verified.")
    except Exception as e:
        print(f"❌ Storage initialization failed: {e}")
        return

    # 2. Data Extraction from Source (NYC Trip Data - January 2024)
    source_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    target_file = "yellow_tripdata_2024-01.parquet"
    
    print(f"🚀 Extracting data from source: {source_url}")
    
    try:
        response = requests.get(source_url, timeout=60) # Increased timeout for reliability
        if response.status_code == 200:
            data_stream = BytesIO(response.content)
            content_length = len(response.content)
            
            # 3. Load to Object Storage (MinIO)
            client.put_object(
                BUCKET_NAME, 
                target_file, 
                data_stream, 
                content_length,
                content_type="application/x-parquet"
            )
            print(f"✅ Ingestion successful: {target_file} persisted in MinIO.")
        else:
            print(f"❌ Extraction failed. HTTP Status Code: {response.status_code}")
    except Exception as e:
        print(f"❌ Critical error during Ingestion process: {e}")

if __name__ == "__main__":
    download_and_upload()