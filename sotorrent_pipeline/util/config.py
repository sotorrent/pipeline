import json
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from pkg_resources import resource_stream

LOG_LEVEL = logging.INFO
logger = logging.getLogger()

BIGQUERY_SCHEMAS = []


def load_bigquery_schemas():
    if len(BIGQUERY_SCHEMAS) > 0:
        return
    for table_name in GOOGLE_CLOUD_PIPELINE.get('bigquery_table_names'):
        schema_json = resource_stream('sotorrent_pipeline', f'bigquery_schemas/{table_name}.json').read().decode()
        logger.info(f"Reading schema file for table '{table_name}'")
        BIGQUERY_SCHEMAS.append(json.loads(schema_json))
    logger.info(f"Read {len(BIGQUERY_SCHEMAS)} schema file(s).")


LOCAL_PIPELINE = {
    'input': '/Users/sebastian/git/sotorrent/pipeline/so_dump/Posts.xml',
    'output': '/Users/sebastian/git/sotorrent/pipeline/output/Posts.jsonl',
    'pipeline_options': PipelineOptions(
        runner='DirectRunner'
    )
}

GOOGLE_CLOUD_PIPELINE = {
    'input': 'gs://sotorrent_pipeline/so_dump/Posts.xml',
    'output': 'gs://sotorrent_pipeline/output/Posts.jsonl',
    'bigquery_table_names': ['Posts'],
    'pipeline_options': PipelineOptions(
        runner='DataflowRunner',
        project='sotorrent-org',
        region='us-central1',
        temp_location='gs://sotorrent_pipeline/temp/',
        staging_location='gs://sotorrent_pipeline/staging/',
        job_name='sotorrent-pipeline-test'
    )
}


ACTIVE_PIPELINE = LOCAL_PIPELINE
SAVE_MAIN_SESSION = True
