import json
import logging
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from sotorrent import config

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self):
        self.options = PipelineOptions(
            runner=config.yaml['google-cloud']['pipeline-options']['runner'],
            project=config.yaml['google-cloud']['pipeline-options']['project'],
            job_name=config.yaml['google-cloud']['pipeline-options']['job_name'],
            temp_location=config.yaml['google-cloud']['pipeline-options']['temp_location'],
            region=config.yaml['google-cloud']['pipeline-options']['region']
        )
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.key_file
        self.table_schemas = []

    def run(self):
        self._load_table_schemas()

        with beam.Pipeline(options=self.options) as p:
            pass

    def _load_table_schemas(self):
        for file in os.listdir(config.schema_dir):
            file_name, file_extension = os.path.splitext(file)
            if not file_extension == ".json":
                continue
            logger.info(f"Reading schema file for table {file_name}")
            with open(os.path.join(config.schema_dir, file), 'r') as schema_file:
                self.table_schemas.append(json.loads(schema_file.read()))
        logger.info(f"Read {len(self.table_schemas)} schema files.")
