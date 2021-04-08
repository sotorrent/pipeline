import logging
import os

from google.cloud import storage
from sotorrent_pipeline.util.log import initialize_logger


def get_storage_client():
    return


def upload_xml(input_file, bucket_name, bucket_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    upload_xml('/Users/sebastian/git/sotorrent/pipeline/so_dump/Posts.xml', 'sotorrent_pipeline', 'so_dump/Posts.xml')
else:
    logger = logging.getLogger(__name__)
