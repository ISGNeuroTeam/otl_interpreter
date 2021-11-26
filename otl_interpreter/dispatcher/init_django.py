############################################################################
## Django ORM Standalone Python Template
############################################################################
import sys
import os
import django


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


    os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_module)
    django.setup()

