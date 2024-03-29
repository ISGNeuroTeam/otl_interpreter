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
    READY_TO_EXECUTE = 'READY_TO_EXECUTE', _('Ready to execute')  # when all children jobs are done
    IN_QUEUE = 'IN_QUEUE', _('In queue')
    TAKEN_FROM_QUEUE = 'TAKEN_FROM_QUEUE', _('Taken from queue')
    SENT_TO_COMPUTING_NODE = 'SENT_TO_COMPUTING_NODE', _('Sent to computing node')
    DECLINED_BY_COMPUTING_NODE = 'DECLINED_BY_COMPUTING_NODE', _('Declined by computing node')
    RUNNING = 'RUNNING', _('Running')
    WAITING_SAME_RESULT = 'WAITING_SAME_RESULT', _('Waiting the same result from other node job')
    FINISHED = 'FINISHED', _('Finished')
    CANCELED = 'CANCELED', _('Canceled')
    FAILED = 'FAILED', _('Failed')


class ResultStatus(models.TextChoices):
    CALCULATING = 'CALCULATING', _('Result is calculating')
    CALCULATED = 'CALCULATED', _('Result is calculated')
    NOT_EXIST = 'NOT_EXIST', _('Result do not exist')


END_STATUSES = {NodeJobStatus.CANCELED, NodeJobStatus.FAILED, NodeJobStatus.FINISHED}


class ComputingNodeType(models.TextChoices):
    SPARK = 'SPARK', _('Spark')
    EEP = 'EEP', _('EEP')
    POST_PROCESSING = 'POST_PROCESSING', _('Post processing')


class ResultStorage(models.TextChoices):
    INTERPROCESSING = 'interproc_storage', _('Interprocessing storage')
    LOCAL_POST_PROCESSING = 'local_post_processing', _('Local post processing storage')
    SHARED_POST_PROCESSING = 'shared_post_processing', _('Shared post processing storage')