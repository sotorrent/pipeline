import logging
import os
import re
import sys
import sotorrent_pipeline.util.config as config

from google.cloud import storage, bigquery
from sotorrent_pipeline.util.log import initialize_logger


def credentials_set():
    """
    Check
    :return:
    """
    return 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ


def upload_xml_files_to_bucket():
    """
    Upload configured XML files to the configured Google Cloud bucket.
    :return: None
    """
    for table_name in config.TABLES:
        local_input_dir = config.LOCAL_PIPELINE.get('input_dir')
        cloud_storage_output_dir = config.GOOGLE_CLOUD_PIPELINE.get('input_dir')
        file_name = table_name + '.xml'
        bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
        output_path = _get_output_path_from_url(cloud_storage_output_dir, file_name)
        _upload_xml_file_to_bucket(os.path.join(local_input_dir, file_name), bucket_name, output_path)


def rename_jsonl_files_in_bucket():
    """
    Rename JSONL files in configured Google Cloud bucket (remove numbering added due to workers/sharding).
    :return: None
    """
    if not config.ACTIVE_PIPELINE == config.GOOGLE_CLOUD_PIPELINE:
        logger.info("Google Cloud pipeline not active, can't rename files in bucket.")
    config.generate_file_paths()
    cloud_storage_output_dir = config.GOOGLE_CLOUD_PIPELINE.get('output_dir')
    bucket_name = _get_bucket_name_from_url(cloud_storage_output_dir)
    for table_name, output_path in config.OUTPUT_PATHS.items():
        file_name = table_name + '-00000-of-00001.jsonl'
        output_path = _get_output_path_from_url(cloud_storage_output_dir, file_name)
        _rename_jsonl_file_in_bucket(bucket_name, output_path)


def load_jsonl_files_into_bigquery_table():
    """
    Load JSONL files in configured bucket into corresponding BigQuery tables.
    :return: None
    """
    if not config.ACTIVE_PIPELINE == config.GOOGLE_CLOUD_PIPELINE:
        logger.info("Google Cloud pipeline not active, can't load JSONL files from bucket.")
    config.load_bigquery_schemas(with_fields=False)
    config.generate_file_paths()
    for table_name, output_path in config.OUTPUT_PATHS.items():
        input_file = output_path
        destination_table = f"{config.GOOGLE_CLOUD_PIPELINE.get('bigquery_dataset')}.{table_name}"
        table_schema = config.BIGQUERY_SCHEMAS[table_name]
        _load_jsonl_file_into_bigquery_table(input_file, table_schema, destination_table)

def print_job_errors(job_id):
    """
    Helper method to retrieve error information about BigQuery jobs.
    :param job_id: ID of the job.
    :return: None
    """
    bigquery_client = bigquery.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
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


def _get_output_path_from_url(gs_url, file_name):
    matches = re.findall('gs://[^/]+/(.+)', gs_url, flags=re.IGNORECASE)
    if len(matches) == 1:
        output_path = os.path.join(matches[0], file_name)
    else:
        output_path = file_name
    return output_path


def _upload_xml_file_to_bucket(input_file, bucket_name, output_file):
    logger.info(f"Uploading file '{input_file}' to bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.upload_from_filename(input_file)
    logger.info(f"Upload of file '{input_file}' to bucket '{bucket_name}' complete.")


def _rename_jsonl_file_in_bucket(bucket_name, bucket_file):
    logger.info(f"Renaming file '{bucket_file}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)
    new_name = re.sub('-(\\d){5}-of-(\\d){5}', '', bucket_file)  # e.g. Posts-00000-of-00001.jsonl -> Posts.jsonl
    bucket.rename_blob(blob, new_name)
    logger.info(f"File '{bucket_file}' in bucket '{bucket_name} renamed to '{new_name}'.")


def _load_jsonl_file_into_bigquery_table(input_file, json_schema, destination_table):
    logger.info(f"Loading file '{input_file}' into table '{destination_table}'.")
    bigquery_client  = bigquery.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = _json_to_schema_fields(json_schema)
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_EMPTY
    job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
    job = bigquery_client.load_table_from_uri(input_file, destination_table, job_config=job_config)
    if job.errors:
        logger.info(f"Load job for file '{input_file}' into table '{destination_table}' failed.")
        _print_job_errors(job)
    else:
        logger.info(f"Load job for file '{input_file}' into table '{destination_table}' finished.")


def _json_to_schema_fields(schema_json):
    schema_fields = []
    for row in schema_json:
        schema_fields.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return schema_fields


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    upload_xml_files_to_bucket()
    #print_job_errors('beam_bq_job_LOAD_sotorrentpipeline_LOAD_STEP_213_956a3bb978fa274a99c880f1d2bfd4d8_3a1d555474bd411ba393b01a5f1c49f6')
else:
    logger = logging.getLogger(__name__)
