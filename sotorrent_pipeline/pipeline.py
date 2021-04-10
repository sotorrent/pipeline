import argparse
import logging
import os
import sys
import apache_beam as beam
import sotorrent_pipeline.util.config as config
from sotorrent_pipeline.util.google_cloud import credentials_set

from sotorrent_pipeline.util.google_cloud import rename_jsonl_files_in_bucket, load_jsonl_files_into_bigquery_table
from sotorrent_pipeline.util.log import initialize_logger
from sotorrent_pipeline.util.beam import WriteToJson, filter_rows, xml_attributes_to_dict


def run():
    """
    Execute the SOTorrent pipeline (either locally with limited functionality or in Google Cloud).
    :return: None
    """
    input_paths = config.INPUT_PATHS
    output_dir = config.PIPELINE.get('output_dir')

    logger.info(f"Writing output of pipeline to '{output_dir}'")
    for table_name, input_path in input_paths.items():
        logger.info(f"Reading and converting XML file for table '{table_name}' from '{input_path}'...")
        with beam.Pipeline(options=config.get_pipeline_options(table_name)) as p:
            dict_elements = (p
                             | "Read XML file" >> beam.io.ReadFromText(input_path)
                             | "Ignore non-row elements" >> beam.Filter(filter_rows)
                             | "Convert XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict))

            bigquery_dataset = config.PIPELINE['bigquery_dataset']
            logger.info(f"Writing data into BigQuery dataset '{bigquery_dataset}'")
            (dict_elements | "Write data into BigQuery table" >> beam.io.WriteToBigQuery(
                f'{bigquery_dataset}.{table_name}',
                schema=config.BIGQUERY_SCHEMAS[table_name],
                write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

            file_name_without_extension = os.path.join(output_dir, table_name)
            logger.info(f"Writing data to JSONL file '{file_name_without_extension}.jsonl'")
            (dict_elements | "Writing data to JSONL file" >> WriteToJson(file_name_without_extension, num_shards=1))
    logger.info(f"Pipeline finished.")

    rename_jsonl_files_in_bucket()
    load_jsonl_files_into_bigquery_table()


def main():
    """
    Main entry point, reading settings from configuration.
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--setup_file',
        dest='setup_file',
        required=True,
        default=None,
        help='Python setup file.')
    args = parser.parse_args()

    config.SETUP_FILE = args.setup_file
    config.generate_file_paths()

    if not credentials_set():
        error_message = "Google Cloud credentials are not available. Environment variable GOOGLE_APPLICATION_CREDENTIALS set?"
        logger.error(error_message)
        sys.exit(error_message)
    config.load_bigquery_schemas()
    logger.info("Executing SOTorrent pipeline...")
    run()


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)
