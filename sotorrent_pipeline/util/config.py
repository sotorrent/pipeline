import json
import logging
import os

from apache_beam.options.pipeline_options import PipelineOptions
from pkg_resources import resource_stream


LOG_LEVEL = logging.INFO
logger = logging.getLogger()

INPUT_PATHS = dict()
OUTPUT_PATHS = dict()
BIGQUERY_SCHEMAS = dict()


def generate_file_paths():
    if len(INPUT_PATHS) > 0 or len(OUTPUT_PATHS) > 0:
        logger.info("File paths already generated.")
        return
    for table_name in ACTIVE_PIPELINE.get('tables'):
        INPUT_PATHS[table_name] = os.path.join(ACTIVE_PIPELINE.get('input_dir'), f'{table_name}.xml')
        OUTPUT_PATHS[table_name] = os.path.join(ACTIVE_PIPELINE.get('output_dir'), f'{table_name}.jsonl')
    logger.info(f"Generated {len(INPUT_PATHS)} input paths and {len(OUTPUT_PATHS)} output paths.")


def load_bigquery_schemas():
    if len(BIGQUERY_SCHEMAS) > 0:
        logger.info("Schemas already loaded.")
        return
    for table_name in GOOGLE_CLOUD_PIPELINE.get('tables'):
        schema_json = resource_stream('sotorrent_pipeline', f'bigquery_schemas/{table_name}.json').read().decode()
        logger.info(f"Reading schema file for table '{table_name}'")
        BIGQUERY_SCHEMAS[table_name] = json.loads(schema_json)
    logger.info(f"Read {len(BIGQUERY_SCHEMAS)} schema file(s).")


LOCAL_PIPELINE = {
    'input_dir': '/Users/sebastian/git/sotorrent/pipeline/so_dump/',
    'output_dir': '/Users/sebastian/git/sotorrent/pipeline/output/',
    'tables': ['Posts'],
    'pipeline_options': PipelineOptions(
        runner='DirectRunner'
    )
}

GOOGLE_CLOUD_PIPELINE = {
    'input_dir': 'gs://sotorrent_pipeline/so_dump/',
    'output_dir': 'gs://sotorrent_pipeline/output/',
    'tables': ['Posts'],
    'bigquery_dataset': 'sotorrent-org:2021_04_06',
    'pipeline_options': PipelineOptions(
        runner='DataflowRunner',
        project='sotorrent-org',
        region='us-central1',
        temp_location='gs://sotorrent_pipeline/temp/',
        staging_location='gs://sotorrent_pipeline/staging/',
        job_name='sotorrent-pipeline'
    )
}

ACTIVE_PIPELINE = GOOGLE_CLOUD_PIPELINE
SAVE_MAIN_SESSION = True
