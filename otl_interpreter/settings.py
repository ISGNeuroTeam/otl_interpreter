import configparser
import os

from pathlib import Path
from core.settings.ini_config import merge_ini_config_with_defaults


default_ini_config = {
    'logging': {
        'level': 'INFO'
    },
    'db_conf': {
        'host': 'localhost',
        'port': '5433',
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
    'dispatcher': {
        'one_process_mode': False,
        'check_job_queue_period': 10,
        'host_id': os.popen("hostid").read().strip(),
        'health_check_period': 15
    },
    'otl_job_defaults': {
        'cache_ttl': 60,
        'timeout': 0,
        'shared_post_processing': True,

    },
    'result_managing': {
        'data_path': 'data',
        'schema_path': '_SCHEMA',
        'cleaning_period': 30
    },
    'storages': {
        'shared_post_processing': '/opt/otp/shared_storage',
        'local_post_processing': '/opt/otp/local_storage',
        'interproc_storage': '/opt/otp/inter_proc_storage',
    }
}

config_parser = configparser.ConfigParser()

config_parser.read(Path(__file__).parent / 'otl_interpreter.conf')

ini_config = merge_ini_config_with_defaults(config_parser, default_ini_config)

job_planer_config = ini_config['job_planer']

host_id = ini_config['dispatcher']['host_id']

DATABASE = {

        "ENGINE": 'django.db.backends.postgresql',
        "NAME": ini_config['db_conf']['database'],
        "USER": ini_config['db_conf']['user'],
        "PASSWORD": ini_config['db_conf']['password'],
        "HOST": ini_config['db_conf']['host'],
        "PORT": ini_config['db_conf']['port']
}

CELERY_BEAT_SCHEDULE = {
    'delete_old_results': {
        'schedule': 60.0,
        'task': 'otl_interpreter.tasks.delete_old_results',
    },
}


def get_cache():
    # plugin settings load before complex_rest setggings. redis is not configured so
    # import must be here
    from cache import get_cache as complex_rest_get_cache
    return complex_rest_get_cache(
        'RedisCache', namespace='otl_interpreter', timeout=300, max_entries=300
    )
