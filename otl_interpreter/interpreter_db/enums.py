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
    READY_TO_EXECUTE = 'READУ_TO_EXECUTE', _('Ready to execute')  # when all children jobs are done
    IN_QUEUE = 'IN_QUEUE', _('In queue')
    TAKEN_FROM_QUEUE = 'TAKEN_FROM_QUEUE', _('Taken from queue')
    SENT_TO_COMPUTING_NODE = 'SENT_TO_COMPUTING_NODE', _('Sent to computing node')
    DECLINED_BY_COMPUTING_NODE = 'DECLINED_BY_COMPUTING_NODE', _('Declined by computing node')
    RUNNING = 'RUNNING', _('Running')
    FINISHED = 'FINISHED', _('Finished')
    CANCELED = 'CANCELED', _('Canceled')
    FAILED = 'FAILED', _('Failed')


class ComputingNodeType(models.TextChoices):
    SPARK = 'SPARK', _('Spark')
    EEP = 'EEP', _('EEP')
    POST_PROCESSING = 'POST_PROCESSING', _('Post processing')


class ResultStorage(models.TextChoices):
    INTERPROCESSING = 'INTERPROC_STORAGE', _('Interprocessing storage')
    LOCAL_POST_PROCESSING = 'LOCAL_POST_PROCESSING', _('Local post processing storage')
    SHARED_POST_PROCESSING = 'SHARED_POST_PROCESSING', _('Shared post processing storage')