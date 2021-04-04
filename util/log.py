import logging

def initialize_logger(name, log_level):
    """
    Configure a named logger (see https://stackoverflow.com/a/43794480).
    :param name: Name of the logger, will also be use to write DEBUG-level logs to a corresponding log file.
    """

    # create logger for module
    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName(log_level))  # lowest log level the logger will handle (but not necessarily output)

    log_formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')

    # write log messages to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.getLevelName(log_level))
    logger.addHandler(console_handler)
