import logging
import os
import re
import sys
import sotorrent_pipeline.util.config as config

from google.cloud import storage
from sotorrent_pipeline.util.log import initialize_logger


def upload_files_to_bucket():
    for table_name in config.GOOGLE_CLOUD_PIPELINE.get('tables'):
        local_input_dir = config.LOCAL_PIPELINE.get('input_dir')
        cloud_storage_output_dir = config.GOOGLE_CLOUD_PIPELINE.get('input_dir')
        file_name = table_name + '.xml'

        bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
        output_path = _get_output_path_from_url(cloud_storage_output_dir, file_name)

        _upload_xml(os.path.join(local_input_dir, file_name), bucket_name, output_path)


def rename_files_in_bucket():
    if not config.ACTIVE_PIPELINE == config.GOOGLE_CLOUD_PIPELINE:
        logger.info("Google Cloud pipeline not active, can't rename files in bucket.")
    config.generate_file_paths()
    cloud_storage_output_dir = config.GOOGLE_CLOUD_PIPELINE.get('output_dir')
    bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
    for table_name, output_path in config.OUTPUT_PATHS.items():
        file_name = table_name + '-00000-of-00001.jsonl'
        output_path = _get_output_path_from_url(cloud_storage_output_dir, file_name)
        _rename_jsonl_file(bucket_name, output_path)


def _get_bucket_name_from_url(gs_url):
    matches = re.findall('gs://([^/]+)', gs_url, flags=re.IGNORECASE)
    if len(matches) != 1:
        error_message = f"Invalid Google Storage URL: {gs_url}"
        logger.error(error_message)
        sys.exit(error_message)
    return matches[0]


def _get_output_path_from_url(gs_url, file_name):
    matches = re.findall('gs://[^/]+/(.+)', gs_url, flags=re.IGNORECASE)
    if len(matches) == 1:
        output_path = os.path.join(matches[0], file_name)
    else:
        output_path = file_name
    return output_path


def _upload_xml(input_file, bucket_name, bucket_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


def _rename_jsonl_file(bucket_name, bucket_file):
    logger.info(f"Renaming file '{bucket_file}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    new_name = re.sub('-(\\d){5}-of-(\\d){5}', '', bucket_file)  # e.g. Posts-00000-of-00001.jsonl -> Posts.jsonl
    bucket.rename_blob(blob, new_name)
    logger.info(f"File '{bucket_file}' in bucket '{bucket_name} renamed to '{new_name}'.")


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    upload_files_to_bucket()
else:
    logger = logging.getLogger(__name__)
