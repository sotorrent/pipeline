import logging
import os
import requests

from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)


def upload_jsonl_files_to_zenodo_bucket(config):
    logger.info(f"Uploading compressed JSONL files to Zenodo deposit '{config.zenodo_deposit}'...")
    bucket_url = _get_zenodo_bucket_id(config)
    for table_name in config.tables + config.type_tables:
        local_output_dir = config.pipeline['local_output_dir']
        file_path = os.path.join(local_output_dir, table_name + '.jsonl.bz2')
        _upload_jsonl_file_to_zenodo_bucket(config, file_path, bucket_url)


def _upload_jsonl_file_to_zenodo_bucket(config, file_path, bucket_url):
    file_name = os.path.basename(file_path)
    logger.info(f"Uploading '{file_path}' to bucket '{bucket_url}'...")
    params = {'access_token': config.zenodo_access_token}
    with open(file_path, "rb") as fp:
        request = _retry_on_connection_error(lambda: requests.put(
            f'{bucket_url}/{file_name}',
            data=fp,
            params=params,
        ))
        logger.info(f"Upload complete: '{request.json()}'")


def _get_zenodo_bucket_id(config):
    logger.info(f"Getting bucket URL for Zenodo deposit '{config.zenodo_deposit}'...")
    headers = {'Content-Type': 'application/json'}
    params = {'access_token': config.zenodo_access_token}
    request = _retry_on_connection_error(lambda: requests.get(f'https://zenodo.org/api/deposit/depositions/{config.zenodo_deposit}',
                      params=params,
                      json={},
                      headers=headers))
    return request.json()["links"]["bucket"]


def _retry_on_connection_error(function_to_execute, max_retries=5):
    retries = 0
    connection_error = None
    while retries < max_retries:
        try:
            return function_to_execute()
        except ConnectionError as e:
            retries += 1
            logger.warning(f"Connection error, retrying (attempt {retries} of {max_retries})...")
            connection_error = e
    logger.error("Maximum retries exceeded, aborting.")
    raise connection_error
