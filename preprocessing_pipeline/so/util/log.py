import logging

from preprocessing_pipeline.so.util.config import LOG_LEVEL


def initialize_logger(logger_name):
    """
    Configure a named logger (see https://stackoverflow.com/a/43794480).
    """

    # create logger for module
    module_logger = logging.getLogger(logger_name)
    # set lowest log level the logger will handle (but not necessarily output)
    module_logger.setLevel(LOG_LEVEL)
    # disable propagation to root logger
    module_logger.propagate = False

    log_formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')

    # write log messages to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(LOG_LEVEL)
    module_logger.addHandler(console_handler)

    return module_logger
