############################################################################
## Django ORM Standalone Python Template
############################################################################
import sys
import os
import django
import logging

log = logging.getLogger('otl_interpreter.dispatcher')


def init_django(settings_module=None):
    """
    Here we'll import the parts of Django we need. It's recommended to leave
    these settings as is, and skip to START OF APPLICATION section below
    """

    # Turn off bytecode generation
    sys.dont_write_bytecode = True

    # Django specific settings

    if len(sys.argv) > 1:
        settings_module = sys.argv[1]
    else:
        settings_module = 'core.settings'

    log.debug(f'Getting django settings: {settings_module}')

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_module)
    django.setup()
