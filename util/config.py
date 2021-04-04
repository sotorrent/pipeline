import os
import yaml

from pathlib import Path


class Config:
    def __init__(self):
        self.config_file = Config._get_abs_path('config.yml')
        self.yaml = self._parse_yaml_config()
        self.key_file = Config._get_abs_path(self.yaml['google-cloud']['key_file'])
        self.schema_dir = Config._get_abs_path(self.yaml['google-cloud']['schema_dir'])

    def _parse_yaml_config(self):
        with open(self.config_file, 'r') as yaml_file:
            return yaml.load(yaml_file, Loader=yaml.SafeLoader)

    @staticmethod
    def _get_abs_path(file_in_root):
        return os.path.abspath(os.path.join(Path(__file__).parent.parent, file_in_root))
