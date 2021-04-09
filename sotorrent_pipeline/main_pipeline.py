import logging
import os
import sys
import apache_beam as beam
import sotorrent_pipeline.util.config as config

from apache_beam.options.pipeline_options import SetupOptions
from sotorrent_pipeline.util.log import initialize_logger
from sotorrent_pipeline.util.beam import XmlToDict, WriteToJson, JsonlToDict
from sotorrent_pipeline.util.google_cloud import rename_files_in_bucket


def run():
    input_paths = config.INPUT_PATHS
    output_dir = config.ACTIVE_PIPELINE.get('output_dir')
    pipeline_options = config.ACTIVE_PIPELINE.get('pipeline_options')

    logger.info(f"Writing output of first pipeline to {output_dir}")
    for table_name, input_path in input_paths.items():
        logger.info(f"Reading and converting XML file for table '{table_name}' from '{input_path}'...")
        with beam.Pipeline(options=pipeline_options) as p:
            (p
             | "Read XML files" >> beam.Create([input_path])
             | "Convert XML files to dict elements" >> beam.FlatMap(lambda xml_file:
                                                                         iter(XmlToDict(xml_file).parse_into_dict()))
             | "Convert dict elements to JSONL files" >> WriteToJson(os.path.join(output_dir, table_name),
                                                                     file_name_suffix='.jsonl',
                                                                     write_jsonl=True)
             )
    logger.info("First pipeline finished.")

    if config.ACTIVE_PIPELINE == config.GOOGLE_CLOUD_PIPELINE:
        logger.info("Renaming JSONL files...")
        rename_files_in_bucket()

        bigquery_dataset = config.ACTIVE_PIPELINE['bigquery_dataset']
        output_paths = config.OUTPUT_PATHS
        logger.info(f"Writing output of second pipeline to {bigquery_dataset}")
        for table_name, output_path in output_paths.items():
            logger.info(f"Loading table '{table_name}' from '{output_path}'...")
            with beam.Pipeline(options=pipeline_options) as p:
                (p
                 | "Open JSONL file" >> beam.Create([output_path])
                 | "Read content of JSONL file" >> beam.FlatMap(lambda jsonl_file:
                                                                iter(JsonlToDict(jsonl_file).parse_into_dict()))
                 | 'Write data into BigQuery table' >> beam.io.WriteToBigQuery(
                            f'{bigquery_dataset}.{table_name}',
                            schema=config.BIGQUERY_SCHEMAS[table_name],
                            write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                 )

    logger.info("Second pipeline finished.")


def main():
    if config.SAVE_MAIN_SESSION:
        config.ACTIVE_PIPELINE.get('pipeline_options').view_as(SetupOptions).save_main_session = True

    config.generate_file_paths()

    if config.ACTIVE_PIPELINE == config.LOCAL_PIPELINE:
        logger.info("Executing local pipeline...")
        run()
    elif config.ACTIVE_PIPELINE == config.GOOGLE_CLOUD_PIPELINE:
        config.load_bigquery_schemas()
        logger.info("Executing Google Cloud pipeline...")
        run()
    else:
        error_message = f"Unknown pipeline '{config.ACTIVE_PIPELINE}'"
        logger.error(error_message)
        sys.exit(error_message)


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)
