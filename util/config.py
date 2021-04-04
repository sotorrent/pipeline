import yaml

def parse_yaml_config(file):
    with open(file, 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.SafeLoader)
