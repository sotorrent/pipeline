import json
import os
import sys

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from sotorrent import initialize_logger, load_yaml_config
from sotorrent.json_sink import WriteToJson
from sotorrent.xml_to_json import parse_xml_into_dict

logger = None

GOOGLE_CLOUD = 'google-cloud'
LOCAL = 'local'


def load_bigquery_schemas(schema_dir):
    bigquery_schemas = []
    for file in os.listdir(schema_dir):
        file_name, file_extension = os.path.splitext(file)
        if not file_extension == ".json":
            continue
        logger.info(f"Reading schema file for table '{file_name}'")
        with open(os.path.join(schema_dir, file), 'r') as schema_file:
            bigquery_schemas.append(json.loads(schema_file.read()))
    logger.info(f"Read {len(bigquery_schemas)} schema files.")
    return bigquery_schemas


def load_pipeline_options(yaml_config):
    pipeline_options = None
    if yaml_config['pipelines']['active-pipeline'] == 'local':
        pipeline_options = PipelineOptions()
    elif yaml_config['pipelines']['active-pipeline'] == 'google-cloud':
        pipeline_options = PipelineOptions(
            runner=yaml_config['pipelines']['google-cloud']['pipeline-options']['runner'],
            project=yaml_config['pipelines']['google-cloud']['pipeline-options']['project'],
            job_name=yaml_config['pipelines']['google-cloud']['pipeline-options']['job-name'],
            region=yaml_config['pipelines']['google-cloud']['pipeline-options']['region']
        )
        pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options


def run(input_path, output_path, pipeline_options):
    file_path, file_ext = os.path.splitext(output_path)
    with beam.Pipeline(options=pipeline_options) as p:
        output = (p
                  | "Reading XML file..." >> beam.Create([input_path])
                  | "Converting XML file to dict elements..." >> beam.FlatMap(lambda xml_file:
                                                                              iter(parse_xml_into_dict(xml_file)))
                  | "Converting dict elements to JSON..." >> WriteToJson(file_path,
                                                                         file_name_suffix=file_ext,
                                                                         write_jsonl=True))
        logger.info(output)


if __name__ == '__main__':
    config = load_yaml_config('config.yml')

    logger = initialize_logger(__name__, config['logging']['log-level'])
    active_pipeline = config['pipelines']['active-pipeline']

    if active_pipeline not in [LOCAL, GOOGLE_CLOUD]:
        error_message = f"Unknown pipeline '{active_pipeline}'"
        logger.error(error_message)
        sys.exit(error_message)

    input_path = config['pipelines'][active_pipeline]['input']
    output_path = config['pipelines'][active_pipeline]['output']

    if active_pipeline == GOOGLE_CLOUD:
        load_bigquery_schemas(config['pipelines'][GOOGLE_CLOUD]['bigquery-schema-dir'])
        bucket, _ = os.path.split(input_path)

    run(input_path, output_path, load_pipeline_options(config))
