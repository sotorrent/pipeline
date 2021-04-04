import logging
import os

from sotorrent import config
from google.cloud import storage

logger = logging.getLogger(__name__)

def upload_xml(file):
    bucket_name = config['upload']['bucket']
    json_key_file_path = config['upload']['json-key-file']

    logger.info(f"Uploading file {file} to bucket {bucket_name}...")
    storage_client = storage.Client.from_service_account_json(json_key_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(file))
    blob.upload_from_filename(file)

    logger.info(f"Upload of file {file} to bucket {bucket_name} complete.")
