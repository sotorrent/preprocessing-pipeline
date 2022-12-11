# Preprocessing Pipeline

This pipeline reads XML files from the official Stack Exchange 
[data dump](https://archive.org/details/stackexchange) and extracts code blocks into JSONL files.

To run the pipeline in Google Cloud, you need to set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS="$PWD/google-cloud-key.json"

First, you need to install the preprocessing_pipeline package:

    pip3 install .

Then, you can run the pipeline:

    preprocessing-pipeline --config_file "$PWD/config.json"
