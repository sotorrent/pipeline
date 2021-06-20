# SOTorrent Pipeline

The SOTorrent pipeline currently only reads XML files from the official Stack Overflow 
[data dump](https://archive.org/details/stackexchange) into Google BigQuery tables and JSONL files (for export).
This pipeline will be extended to create the whole SOTorrent dataset.

To run the pipeline in Google Cloud, you need to set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS="$PWD/google-cloud-key.json"

First, you need to install the sotorrent_pipeline package:

    python3 setup.py install

Then, you can upload the XML files to a Google Cloud storage bucket:

    sotorrent-pipeline --mode upload --config_file "/Users/sebastian/git/sotorrent/pipeline/config.json"

Then, you can run the pipeline:

    sotorrent-pipeline --mode pipeline --config_file "/Users/sebastian/git/sotorrent/pipeline/config.json"

To inspect a failed BigQuery job, you can run:

    sotorrent-pipeline --mode debug --config_file "/Users/sebastian/git/sotorrent/pipeline/config.json" --job_id <JOB_ID>

To upload the JSONL files to a Zenodo bucket, you need to set the following environment variable:

    export ZENODO_TOKEN="..."
