import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from sotorrent import config

logger = logging.getLogger(__name__)

class Pipeline:

    def __init__(self):
        self.options = PipelineOptions(
            runner=config['beam']['runner'],
            project=config['beam']['project'],
            job_name=config['beam']['job_name'],
            temp_location=config['beam']['temp_location'],
            staging_location=config['beam']['staging_location'],
            region=config['beam']['region']
        )

    def run(self):
        with beam.Pipeline(options=self.options) as p:
            pass