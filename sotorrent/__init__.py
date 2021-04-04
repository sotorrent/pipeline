from util.config import Config
from util.log import initialize_logger

config = Config()

# configure root logger for module
initialize_logger(__name__, config.yaml['logging']['log-level'])
