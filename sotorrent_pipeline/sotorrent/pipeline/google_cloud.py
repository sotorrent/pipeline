import logging
import os
import re
import sys

from google.cloud import storage, bigquery

logger = logging.getLogger(__name__)


def upload_xml_files_to_bucket(config):
    """
    Upload SO dump XML files to the configured Google Cloud bucket.
    :param config Used to access Google Cloud credentials
    :return: None
    """
    for table_name in config.tables:
        local_input_dir = config.pipeline['local_input_dir']
        cloud_storage_output_dir = config.pipeline['input_dir']
        file_name = table_name + '.xml'
        bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
        input_path = os.path.join(local_input_dir, file_name)
        output_path = _get_path_from_gs_url(cloud_storage_output_dir, file_name)
        _upload_file_to_bucket(config, input_path, bucket_name, output_path)


def upload_type_table_files_to_bucket(config):
    """
    Upload JSONL files for type tables to the configured Google Cloud bucket.
    :param config Used to access Google Cloud credentials
    :return: None
    """
    for table_name in config.type_tables:
        cloud_storage_output_dir = config.pipeline['output_dir']
        file_name = table_name + '.jsonl'
        bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
        output_path = _get_path_from_gs_url(cloud_storage_output_dir, file_name)
        _upload_string_to_bucket(config, config.type_tables_jsonl[table_name], bucket_name, output_path)


def load_type_tables_into_bigquery(config):
    """
    Load type table JSONL files from the Google Cloud bucket into BigQuery tables.
    :param config Used to access Google Cloud credentials
    :return: None
    """
    for table_name in config.type_tables:
        cloud_storage_output_dir = config.pipeline['output_dir']
        file_name = table_name + '.jsonl'
        input_path = os.path.join(cloud_storage_output_dir, file_name)
        schema = config.bigquery_schemas[table_name]
        output_table = f"{config.pipeline['bigquery_dataset']}.{table_name}"
        _load_jsonl_file_into_bigquery_table(config, input_path, schema, output_table)


def rename_jsonl_files_in_bucket(config):
    """
    Rename JSONL files in configured Google Cloud bucket (remove numbering added due to workers/sharding).
    :param config Used to access Google Cloud credentials
    :return: None
    """
    cloud_storage_output_dir = config.pipeline['output_dir']
    bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
    for table_name, output_path in config.output_paths.items():
        file_name = table_name + '-00000-of-00001.jsonl'
        output_path = _get_path_from_gs_url(cloud_storage_output_dir, file_name)
        _rename_jsonl_file_in_bucket(config, bucket_name, output_path)


def download_jsonl_files_from_bucket(config):
    """
    Download JSONL files created by the pipeline from the configured Google Cloud bucket
    to the configured local output directory.
    :param config used to access Google Cloud credentials
    :return: None
    """
    for table_name in config.tables + config.type_tables:
        cloud_storage_input_dir = config.pipeline['output_dir']
        local_output_dir = config.pipeline['local_output_dir']
        file_name = table_name + '.jsonl'
        bucket_name = _get_bucket_name_from_url(cloud_storage_input_dir)
        input_path = _get_path_from_gs_url(cloud_storage_input_dir, file_name)
        output_path = os.path.join(local_output_dir, file_name)
        _download_file_from_bucket(config, input_path, bucket_name, output_path)


def print_job_errors(config, job_id):
    """
    Helper method to retrieve error information about BigQuery jobs.
    :param config used to access Google Cloud credentials
    :param job_id: ID of the job.
    :return: None
    """
    bigquery_client = bigquery.Client.from_service_account_json(config.google_credentials_json_file)
    job = bigquery_client.get_job(job_id)
    _print_job_errors(job)


def _print_job_errors(job):
    if not job.errors:
        return
    error_message = f"Errors for '{job.job_type}' job created '{job.created}' with state '{job.state}': "
    for error in job.errors:
        error_message += f"(reason: {error['reason']}; message: {error['message']}"
        if 'location' in error:
            error_message += f"; location: {error['location']}"
        error_message += ") "
    logger.error(error_message)


def _get_bucket_name_from_url(gs_url):
    matches = re.findall('gs://([^/]+)', gs_url, flags=re.IGNORECASE)
    if len(matches) != 1:
        error_message = f"Invalid Google Storage URL: '{gs_url}'"
        logger.error(error_message)
        sys.exit(error_message)
    return matches[0]


def _get_path_from_gs_url(gs_url, file_name):
    matches = re.findall('gs://[^/]+/(.+)', gs_url, flags=re.IGNORECASE)
    if len(matches) == 1:
        output_path = os.path.join(matches[0], file_name)
    else:
        output_path = file_name
    return output_path


def _upload_string_to_bucket(config, input_string, bucket_name, output_file):
    logger.info(f"Uploading file '{output_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(config.google_credentials_json_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.upload_from_string(input_string)
    logger.info(f"Upload of file '{output_file}' to bucket '{bucket_name}' complete.")


def _upload_file_to_bucket(config, input_file, bucket_name, output_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(config.google_credentials_json_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


def _download_file_from_bucket(config, input_file, bucket_name, output_file):
    logger.info(f"Downloading file '{input_file}' from bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(config.google_credentials_json_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(input_file)
    blob.download_to_filename(output_file)
    logger.info(f"Download of file '{input_file}' from bucket '{bucket_name}' complete.")


def _rename_jsonl_file_in_bucket(config, bucket_name, bucket_file):
    logger.info(f"Renaming file '{bucket_file}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(config.google_credentials_json_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    new_name = re.sub('-(\\d){5}-of-(\\d){5}', '', bucket_file)  # e.g. Posts-00000-of-00001.jsonl -> Posts.jsonl
    bucket.rename_blob(blob, new_name)
    logger.info(f"File '{bucket_file}' in bucket '{bucket_name} renamed to '{new_name}'.")


def _load_jsonl_file_into_bigquery_table(config, input_file, json_schema, output_table):
    logger.info(f"Loading file '{input_file}' into table '{output_table}'.")
    bigquery_client = bigquery.Client.from_service_account_json(config.google_credentials_json_file)
    job = bigquery_client.load_table_from_uri(input_file,
                                              output_table,
                                              job_config=_get_bigquery_job_config(json_schema))
    if job.errors:
        logger.info(f"Load job for file '{input_file}' into table '{output_table}' failed.")
        _print_job_errors(job)
    else:
        logger.info(f"Load job for file '{input_file}' into table '{output_table}' finished.")


def _get_bigquery_job_config(json_schema):
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = _json_to_schema_fields(json_schema)
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_EMPTY
    job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
    return job_config


def _json_to_schema_fields(json_schema):
    schema_fields = []
    for row in json_schema:
        schema_fields.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return schema_fields
