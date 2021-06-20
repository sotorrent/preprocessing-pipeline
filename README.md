# Preprocessing Pipeline

This pipeline reads XML files from the official Stack Exchange 
[data dump](https://archive.org/details/stackexchange) and extracts normalized text blocks into JSONL files.

First, you need to install the preprocessing_pipeline package:

    python3 setup.py install

Then, you can run the pipeline:

    preprocessing-pipeline --config_file "/Users/sebastian/git/sotorrent/preprocessing-pipeline/config.json"
