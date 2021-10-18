from django.utils.translation import gettext_lazy as _
from django.db import models


class JobStatus(models.TextChoices):
    NEW = 'NEW', _('New')
    TRANSLATED = 'TRANSLATED', _('Translated')
    PLANNED = 'PLANNED', _('Planned')
    RUNNING = 'RUNNING', _('Running')
    FINISHED = 'FINISHED', _('Finished')
    CANCELED = 'CANCELED', _('Canceled')
    FAILED = 'FAILED', _('Failed')


class NodeJobStatus(models.TextChoices):
    PLANNED = 'PLANNED', _('Planned')
    IN_QUEUE = 'IN_QUEUE', _('In queue')
    TAKEN_FROM_QUEUE = 'TAKEN_FROM_QUEUE', _('Taken from queue')
    RUNNING = 'RUNNING', _('Running')
    FINISHED = 'FINISHED', _('Finished')
    CANCELED = 'CANCELED', _('Canceled')
    FAILED = 'FAILED', _('Failed')


class CommandType(models.TextChoices):
    REQUIRED_COMMAND = 'REQUIRED_COMMAND', _('Required command')
    MACROS_COMMAND = 'MACROS_COMMAND', _('Command-macros')
    NODE_COMMAND = 'NODE_COMMAND', _('Node command')


class ResourceType(models.TextChoices):
    JOB_CAPACITY = 'JOB_CAPACITY', _('Job capacity')
    COMPUTING_RESOURCE = 'COMPUTING_RESOURCE', _('Computing resource')
    RAM_MEMORY = 'RAM_MEMORY', _('RAM memory')


class NodeType(models.TextChoices):
    SPARK = 'SPARK', _('Spark')
    EEP = 'EEP', _('EEP')
    POST_PROCESSING = 'POST_PROCESSING', _('Post processing')


class ResultStorage(models.TextChoices):
    INTERPROCESSING = 'INTERPROC_STORAGE', _('Interprocessing storage')
    LOCAL_POST_PROCESSING = 'LOCAL_POST_PROCESSING', _('Local post processing storage')
    SHARED_POST_PROCESSING = 'SHARED_POST_PROCESSING', _('Shared post processing storage')