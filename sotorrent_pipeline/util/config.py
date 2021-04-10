import copy
import json
import logging
import os

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from pkg_resources import resource_stream


LOG_LEVEL = logging.INFO
logger = logging.getLogger()

INPUT_PATHS = dict()
OUTPUT_PATHS = dict()
BIGQUERY_SCHEMAS = dict()


def generate_file_paths():
    """
    Generate file paths for configured input and output files.
    :return: None
    """
    if len(INPUT_PATHS) > 0 or len(OUTPUT_PATHS) > 0:
        logger.info("File paths already generated.")
        return
    for table_name in TABLES:
        INPUT_PATHS[table_name] = os.path.join(ACTIVE_PIPELINE.get('input_dir'), f'{table_name}.xml')
        OUTPUT_PATHS[table_name] = os.path.join(ACTIVE_PIPELINE.get('output_dir'), f'{table_name}.jsonl')
    logger.info(f"Generated {len(INPUT_PATHS)} input paths and {len(OUTPUT_PATHS)} output paths.")


def load_bigquery_schemas(with_fields=True):
    """
    Load BigQuery table schemas from JSON files.
    Either encapsulated in an object as required by Apache Beam or not
    (as required by the Google Cloud BigQuery package).
    :param with_fields: configure whether the schema should be encapsulated in an object with a fields property
    :return: None
    """
    if len(BIGQUERY_SCHEMAS) > 0:
        logger.info("Schemas already loaded.")
        return
    for table_name in TABLES:
        schema_json = resource_stream('sotorrent_pipeline', f'bigquery_schemas/{table_name}.json').read().decode()
        logger.info(f"Reading schema file for table '{table_name}'")
        if with_fields:
            BIGQUERY_SCHEMAS[table_name] = json.loads('{"fields":' + schema_json + '}')
        else:
            BIGQUERY_SCHEMAS[table_name] = json.loads(schema_json)
    logger.info(f"Read {len(BIGQUERY_SCHEMAS)} schema file(s).")


def get_pipeline_options(table_name):
    """
    Get pipeline options for active pipeline
    :param table_name: Name of currently processed table
    :param job_name:
    :return:
    """
    pipeline_options_dict = copy.deepcopy(ACTIVE_PIPELINE.get('pipeline_options'))
    if ACTIVE_PIPELINE == GOOGLE_CLOUD_PIPELINE:
        pipeline_options_dict['job_name'] =  f"{pipeline_options_dict['job_name']}-{str(table_name).lower()}"
    pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)
    if SAVE_MAIN_SESSION:
        pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options


TABLES = ['Badges', 'Tags', 'PostLinks']

LOCAL_PIPELINE = {
    'input_dir': '/mnt/f/Git/sotorrent/pipeline/so_dump/',
    'output_dir': '/mnt/f/Git/sotorrent/pipeline/output/',
    'pipeline_options': {
        'runner': 'DirectRunner'
    }
}

GOOGLE_CLOUD_PIPELINE = {
    'input_dir': 'gs://sotorrent_pipeline/so_dump/',
    'output_dir': 'gs://sotorrent_pipeline/output/',
    'bigquery_dataset': '2021_04_06',
    'pipeline_options': {
        'runner': 'DataflowRunner',
        'project': 'sotorrent-org',
        'region': 'us-central1',
        'temp_location': 'gs://sotorrent_pipeline/temp/',
        'staging_location': 'gs://sotorrent_pipeline/staging/',
        'job_name': 'sotorrent-pipeline'
    }
}

ACTIVE_PIPELINE = GOOGLE_CLOUD_PIPELINE
SAVE_MAIN_SESSION = True
