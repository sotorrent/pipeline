import logging
import os

from sotorrent import config
from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_xml(file):
    input_bucket = config.yaml['google-cloud']['input_bucket']

    logger.info(f"Uploading file {file} to bucket {input_bucket}...")
    storage_client = storage.Client.from_service_account_json(config.key_file)
    bucket = storage_client.bucket(input_bucket)
    blob = bucket.blob(os.path.basename(file))
    blob.upload_from_filename(file)

    logger.info(f"Upload of file {file} to bucket {input_bucket} complete.")
