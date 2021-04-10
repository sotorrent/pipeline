import copy
import json
import logging
import os

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from pkg_resources import resource_stream

LOG_LEVEL = logging.INFO
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, config_file):
        with open(config_file, mode='r', encoding='utf-8') as fp:
            json_config = json.loads(fp.read())
            self.setup_file = json_config['setup_file']
            self.save_main_session = json_config['save_main_session']
            self.tables = json_config['tables']
            self.pipeline = json_config['pipeline']
            self.input_paths = dict()
            self.output_paths = dict()
            self.bigquery_schemas = dict()
            self.bigquery_schemas_with_fields = dict()

            self._generate_file_paths()
            self._load_bigquery_schemas()

    def _generate_file_paths(self):
        """
        Generate file paths for configured input and output files.
        :return: None
        """
        for table_name in self.tables:
            self.input_paths[table_name] = os.path.join(self.pipeline['input_dir'], f'{table_name}.xml')
            self.output_paths[table_name] = os.path.join(self.pipeline['output_dir'], f'{table_name}.jsonl')
        logger.info(f"Generated {len(self.input_paths)} input paths and {len(self.output_paths)} output paths.")

    def _load_bigquery_schemas(self, with_fields=True):
        """
        Load BigQuery table schemas from JSON files.
        Both encapsulated in an object as required by Apache Beam and without
        (as required by the Google Cloud BigQuery package).
        :return: None
        """
        for table_name in self.tables:
            schema_json = resource_stream('sotorrent_pipeline',
                                          f'bigquery_schemas/{table_name}.json').read().decode()
            logger.info(f"Reading schema file for table '{table_name}'")
            self.bigquery_schemas[table_name] = json.loads(schema_json)
            self.bigquery_schemas_with_fields[table_name] = json.loads('{"fields":' + schema_json + '}')
        logger.info(f"Read {len(self.bigquery_schemas)} schema file(s).")

    def get_pipeline_options(self, table_name):
        """
        Get pipeline options for active pipeline
        :param table_name: Name of currently processed table
        :return:
        """
        pipeline_options_dict = copy.deepcopy(self.pipeline['pipeline_options'])
        pipeline_options_dict['job_name'] = f"{pipeline_options_dict['job_name']}-{str(table_name).lower()}"
        pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)
        pipeline_options.view_as(SetupOptions).setup_file = self.setup_file
        if self.save_main_session:
            pipeline_options.view_as(SetupOptions).save_main_session = True
        return pipeline_options
