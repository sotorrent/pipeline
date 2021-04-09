import logging
import os
import sys
import apache_beam as beam

from apache_beam.options.pipeline_options import SetupOptions
from sotorrent_pipeline.util.log import initialize_logger
from sotorrent_pipeline.util.beam import XmlToDict, WriteToJson
from sotorrent_pipeline.util.config import ACTIVE_PIPELINE, LOCAL_PIPELINE, GOOGLE_CLOUD_PIPELINE, \
    load_bigquery_schemas, SAVE_MAIN_SESSION


def run(input_path, output_path, pipeline_options):
    file_path, file_ext = os.path.splitext(output_path)
    logger.info(f"Writing pipeline output to {output_path}")
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | "Reading XML file..." >> beam.Create([input_path])
         | "Converting XML file to dict elements..." >> beam.FlatMap(lambda xml_file:
                                                                     iter(XmlToDict(xml_file).parse_xml_into_dict()))
         | "Converting dict elements to JSON..." >> WriteToJson(file_path,
                                                                 file_name_suffix=file_ext,
                                                                 write_jsonl=True)
         )
    logger.info("Pipeline finished.")


def main():
    if SAVE_MAIN_SESSION:
        ACTIVE_PIPELINE.get('pipeline_options').view_as(SetupOptions).save_main_session = True

    if ACTIVE_PIPELINE == LOCAL_PIPELINE:
        logger.info("Executing local pipeline...")
        run(LOCAL_PIPELINE.get('input'), LOCAL_PIPELINE.get('output'), LOCAL_PIPELINE.get('pipeline_options'))
    elif ACTIVE_PIPELINE == GOOGLE_CLOUD_PIPELINE:
        load_bigquery_schemas()
        logger.info("Executing Google Cloud pipeline...")
        run(GOOGLE_CLOUD_PIPELINE.get('input'), GOOGLE_CLOUD_PIPELINE.get('output'), GOOGLE_CLOUD_PIPELINE.get('pipeline_options'))
    else:
        error_message = f"Unknown pipeline '{ACTIVE_PIPELINE}'"
        logger.error(error_message)
        sys.exit(error_message)


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)