import argparse
import logging
import os
import sys
import apache_beam as beam

from sotorrent_pipeline.util.config import Config
from sotorrent_pipeline.util.google_cloud import credentials_set
from sotorrent_pipeline.util.google_cloud import rename_jsonl_files_in_bucket, load_jsonl_files_into_bigquery_table
from sotorrent_pipeline.util.log import initialize_logger
from sotorrent_pipeline.util.beam import WriteToJson, filter_rows, xml_attributes_to_dict


def run(config):
    """
    Execute the SOTorrent pipeline (either locally with limited functionality or in Google Cloud).
    :return: None
    """
    input_paths = config.input_paths
    output_dir = config.pipeline['output_dir']

    logger.info(f"Writing output of pipeline to '{output_dir}'")
    for table_name, input_path in input_paths.items():
        logger.info(f"Reading and converting XML file for table '{table_name}' from '{input_path}'...")
        with beam.Pipeline(options=config.get_pipeline_options(table_name)) as p:
            dict_elements = (p
                             | "Read XML file" >> beam.io.ReadFromText(input_path)
                             | "Ignore non-row elements" >> beam.Filter(filter_rows)
                             | "Convert XML attributes to dict elements" >> beam.Map(xml_attributes_to_dict))

            bigquery_dataset = config.pipeline['bigquery_dataset']
            logger.info(f"Writing data into BigQuery dataset '{bigquery_dataset}'")
            (dict_elements | "Write data into BigQuery table" >> beam.io.WriteToBigQuery(
                f'{bigquery_dataset}.{table_name}',
                schema=config.bigquery_schemas_with_fields[table_name],
                write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

            file_name_without_extension = os.path.join(output_dir, table_name)
            logger.info(f"Writing data to JSONL file '{file_name_without_extension}.jsonl'")
            (dict_elements | "Writing data to JSONL file" >> WriteToJson(file_name_without_extension, num_shards=1))
    logger.info(f"Pipeline finished.")

    rename_jsonl_files_in_bucket(config)
    load_jsonl_files_into_bigquery_table(config)


def main():
    """
    Main entry point, reading settings from configuration.
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_file',
        dest='config_file',
        required=True,
        default=None,
        help='JSON config file.')
    args = parser.parse_args()

    config = Config(args.config_file)

    if not credentials_set():
        error_message = "Google Cloud credentials are not available. Environment variable GOOGLE_APPLICATION_CREDENTIALS set?"
        logger.error(error_message)
        sys.exit(error_message)

    logger.info("Executing SOTorrent pipeline...")
    run(config)


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)
