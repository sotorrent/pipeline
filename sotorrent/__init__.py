from sotorrent.config import load_yaml_config
from sotorrent.log import initialize_logger

# configure root logger for package
logger = initialize_logger(__name__, load_yaml_config('config.yml')['logging']['log-level'])
