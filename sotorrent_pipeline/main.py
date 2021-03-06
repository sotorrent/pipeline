import argparse
import logging

from sotorrent_pipeline.sotorrent.pipeline.beam import run_pipeline
from sotorrent_pipeline.sotorrent.util.config import Config
from sotorrent_pipeline.sotorrent.pipeline.google_cloud import upload_xml_files_to_bucket, print_job_errors, \
    rename_jsonl_files_in_bucket, download_jsonl_files_from_bucket, upload_type_table_files_to_bucket, \
    load_type_tables_into_bigquery
from sotorrent_pipeline.sotorrent.util.log import initialize_logger
from sotorrent_pipeline.sotorrent.util.zenodo import upload_jsonl_files_to_zenodo_bucket


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
    parser.add_argument(
        '--mode',
        dest='mode',
        required=True,
        default='pipeline',
        help="Mode can either be 'pipeline', 'google-upload', 'zenodo-upload', or 'debug'.")
    parser.add_argument(
        '--job_id',
        dest='job_id',
        required=False,
        default=None,
        help="Job ID for debug purposes.")
    args = parser.parse_args()

    config = Config(args.config_file, args.mode)

    if args.mode == 'google-upload':
        logger.info("Uploading XML files to Google Cloud storage bucket...")
        upload_xml_files_to_bucket(config)
    elif args.mode == 'pipeline':
        logger.info("Executing SOTorrent pipeline...")
        run_pipeline(config)
        logger.info("Renaming generated JSONL files...")
        rename_jsonl_files_in_bucket(config)
        logger.info("Uploading type table JSONL files...")
        upload_type_table_files_to_bucket(config)
        logger.info("Loading type tables into BigQuery...")
        load_type_tables_into_bigquery(config)
        logger.info("Downloading JSONL files...")
        download_jsonl_files_from_bucket(config)
    elif args.mode == 'debug':
        if args.job_id is None:
            logger.error('Job ID needs to be set in debug mode.')
        logger.info(f"Printing errors messages for job '{args.job_id}'...")
        print_job_errors(config, args.job_id)
    elif args.mode == 'zenodo_upload':
        logger.info("Uploading JSONL files to Zenodo bucket...")
        upload_jsonl_files_to_zenodo_bucket(config)
    logger.info("Done.")


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)
