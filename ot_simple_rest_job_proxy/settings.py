import configparser

from pathlib import Path
from core.settings.ini_config import merge_ini_config_with_defaults

default_ini_config = {
    'ot_simple_rest': {
        'url': 'http://localhost:50000',
        'makejob_urn': 'api/makejob',
        'checkjob_urn': 'api/checkjob',
        'getresult_urn': 'api/getresult',
        'secret_key': '8b62abb2-bbf6-4e0e-a7c1-2e4734bebbd9',
    }
}

config_parser = configparser.ConfigParser()

config_parser.read(Path(__file__).parent / 'ot_simple_rest_job_proxy.conf')

# convert to dictionary
config = {s: dict(config_parser.items(s)) for s in config_parser.sections()}

ini_config = merge_ini_config_with_defaults(config, default_ini_config)

# configure your own database if you need
# DATABASE = {
#         "ENGINE": 'django.db.backends.postgresql',
#         "NAME": ini_config['db_conf']['database'],
#         "USER": ini_config['db_conf']['user'],
#         "PASSWORD": ini_config['db_conf']['password'],
#         "HOST": ini_config['db_conf']['host'],
#         "PORT": ini_config['db_conf']['port']
# }