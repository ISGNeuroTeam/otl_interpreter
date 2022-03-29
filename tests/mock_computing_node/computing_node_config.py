import json
from pathlib import Path

base_dir = Path(__file__).parent.resolve()


def read_otl_command_syntax(config_file_name):
    """
    Loads json config file from data/command_syntax
    """
    with open(base_dir / 'data' / 'command_syntax' / config_file_name) as f:
        return json.load(f)


def read_computing_node_config(config_file_name):
    """
    Loads json config file from data/computing_nodes_config
    """
    with open(base_dir / 'data' / 'computing_nodes_config' / config_file_name, 'r') as f:
        return json.load(f)
