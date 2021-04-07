import os

from google.cloud import storage

from sotorrent import initialize_logger, load_yaml_config

logger = initialize_logger(__name__, load_yaml_config('config.yml')['logging']['log-level'])


def upload_xml(input_file, bucket_name, bucket_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


if __name__ == '__main__':
    upload_xml('../so-dump/Posts.xml', 'sotorrent_pipeline', 'so-dump/Posts.xml')
