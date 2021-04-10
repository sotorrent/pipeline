# SOTorrent Pipeline

The SOTorrent pipeline currently only reads XML files from the official Stack Overflow 
[data dump](https://archive.org/details/stackexchange) into Google BigQuery tables and JSONL files (for export).
This pipeline will be extended to create the whole SOTorrent dataset.

To run the pipeline, adapt the [configuration](sotorrent_pipeline/util/config.py) and execute:

    python3 --setup_file setup.py

To run the pipeline in Google Cloud, you need to set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS="$PWD/google-cloud-key.json"
