import os
from pathlib import Path

import yaml


def load_yaml_config(config_file):
    with open(_get_abs_path(config_file), 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.SafeLoader)


def _get_abs_path(file_in_root):
    return os.path.abspath(os.path.join(Path(__file__).parent.parent, file_in_root))
