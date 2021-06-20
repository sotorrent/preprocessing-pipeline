import argparse
import logging

from preprocessing_pipeline.so.pipeline.beam import run_pipeline
from preprocessing_pipeline.so.util.config import Config
from preprocessing_pipeline.so.util.log import initialize_logger


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

    logger.info("Executing preprocessing pipeline...")
    run_pipeline(config)
    logger.info("Done.")


if __name__ == '__main__':
    logger = initialize_logger(__name__)
    main()
else:
    logger = logging.getLogger(__name__)
