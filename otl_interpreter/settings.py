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
    'node_job': {
      'cache_ttl': 60
    },
    'job_planer': {
        'computing_node_type_priority': 'SPARK EEP POST_PROCESSING',
        'subsearch_is_node_job': True
    },
    'otl_job_defaults': {
        'cache_ttl': 60,
        'timeout': 0,
        'shared_post_processing': True,
    }
}

config_parser = configparser.ConfigParser()

config_parser.read_dict(default_ini_config)
config_parser.read(Path(__file__).parent / 'otl_interpreter.conf')

ini_config = config_parser

job_planer_config = ini_config['job_planer']


DATABASE = {

        "ENGINE": 'django.db.backends.postgresql',
        "NAME": ini_config['db_conf']['database'],
        "USER": ini_config['db_conf']['user'],
        "PASSWORD": ini_config['db_conf']['password'],
        "HOST": ini_config['db_conf']['host'],
        "PORT": ini_config['db_conf']['port']
}


def get_cache():
    # plugin settings load before complex_rest setggings. redis is not configured so
    # import must be here
    from cache import get_cache as complex_rest_get_cache
    return complex_rest_get_cache(
        'RedisCache', namespace='otl_interpreter', timeout=300, max_entries=300
    )