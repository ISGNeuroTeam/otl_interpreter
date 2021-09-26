import configparser

from pathlib import Path
from core.settings.ini_config import merge_ini_config_with_defaults

default_ini_config = {
    'logging': {
        'level': 'INFO'
    },
    'db_conf': {
        'host': 'localhost',
        'port': '5432',
        'database':  'otl_interpreter',
        'user': 'otl_interpreter',
        'password': 'otl_interpreter'
    },
    'job_planer': {
        'computing_node_type_priority': 'SPARK EEP POST_PROCESSING'
    }
}

config_parser = configparser.ConfigParser()

config_parser.read(Path(__file__).parent / 'otl_interpreter.conf')

ini_config = merge_ini_config_with_defaults(config_parser, default_ini_config)

job_planer_config = ini_config['job_planer']

DATABASE = {
        "ENGINE": 'django.db.backends.postgresql',
        "NAME": ini_config['db_conf']['database'],
        "USER": ini_config['db_conf']['user'],
        "PASSWORD": ini_config['db_conf']['password'],
        "HOST": ini_config['db_conf']['host'],
        "PORT": ini_config['db_conf']['port']
}