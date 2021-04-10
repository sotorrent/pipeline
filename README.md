# SOTorrent Pipeline

The SOTorrent pipeline currently only reads XML files from the official Stack Overflow 
[data dump](https://archive.org/details/stackexchange) into Google BigQuery tables and JSONL files (for export).
This pipeline will be extended to create the whole SOTorrent dataset.

To run the pipeline in Google Cloud, you need to set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS="$PWD/google-cloud-key.json"

Then, install the package and run the pipeline:

    python3 setup.py install
    sotorrent-pipeline --config_file "/Users/sebastian/git/sotorrent/pipeline/config.json"
