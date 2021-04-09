import logging
import os
import re
import sys

import config

from google.cloud import storage
from sotorrent_pipeline.util.log import initialize_logger


def upload_files_to_bucket():
    if len(config.BIGQUERY_SCHEMAS) > 0:
        return
    for table_name in config.GOOGLE_CLOUD_PIPELINE.get('tables'):
        local_input_dir = config.LOCAL_PIPELINE.get('input_dir')
        cloud_storage_output_dir = config.GOOGLE_CLOUD_PIPELINE.get('input_dir')
        file_name = table_name + '.xml'

        matches = re.findall('gs://([^/]+)', cloud_storage_output_dir, flags=re.IGNORECASE)
        if len(matches) != 1:
            error_message = f"Invalid Google Storage URL: {cloud_storage_output_dir}"
            logger.error(error_message)
            sys.exit(error_message)
        bucket_name = matches[0]

        matches = re.findall('gs://[^/]+/(.+)', cloud_storage_output_dir, flags=re.IGNORECASE)
        if len(matches) == 1:
            output_path = os.path.join(matches[0], file_name)
        else:
            output_path = file_name

        _upload_xml(os.path.join(local_input_dir, file_name), bucket_name, output_path)


def _upload_xml(input_file, bucket_name, bucket_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    upload_files_to_bucket()
else:
    logger = logging.getLogger(__name__)
