import logging


def initialize_logger(logger_name, log_level):
    """
    Configure a named logger (see https://stackoverflow.com/a/43794480).
    :param log_level: configured log level
    :param logger_name: Name of the logger, will also be use to write DEBUG-level logs to a corresponding log file.
    """

    # create logger for module
    module_logger = logging.getLogger(logger_name)
    # set lowest log level the logger will handle (but not necessarily output)
    module_logger.setLevel(logging.getLevelName(log_level))

    log_formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] [%(name)s]: %(message)s')

    # write log messages to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.getLevelName(log_level))
    module_logger.addHandler(console_handler)

    return module_logger
