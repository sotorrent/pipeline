from util.config import parse_yaml_config
from util.log import initialize_logger

config = parse_yaml_config('config.yml')

# configure root logger for module
initialize_logger(__name__, config['logging']['log-level'])