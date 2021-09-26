import re
import hashlib
from django.db import models
from django.utils.translation import gettext_lazy as _
from mixins.models import TimeStampedModel


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



class OtlQuery(TimeStampedModel):
    query = models.TextField(null=False)
    query_hash = models.BinaryField(
        max_length=255, db_index=True, null=False
    )
    cache_ttl = models.DurationField()
    user_guid = models.UUIDField(
        unique=True, editable=False, null=False
    )
    preview = models.BooleanField()
    status = models.CharField(
        max_length=255,
        choices=JobStatus.choices, default=JobStatus.NEW
    )

    def save(self, *args, **kwargs):
        self.query_hash = self._get_query_hash()
        super().save(*args, **kwargs)

    def _get_query_hash(self):
        query = self._remove_repeat_spaces(self.query)
        query_hash = hashlib.blake2b(query.encode())
        return query_hash

    class Meta:
        app_label = 'otl_interpreter'

    @staticmethod
    def _remove_repeat_spaces(query):
        return re.sub(r'\s+', ' ', query)


class ComputingNode(models.Model):
    # node type: EEP, Spark, PostProcessing.
    type = models.CharField(
        max_length=255,
        db_index=True,
    )

    # node guid
    guid = models.UUIDField(unique=True)

    active = models.BooleanField(default=True)


class NodeCommand(models.Model):
    # if node is null then command is required must be implemented on every node
    # or it is macros command
    node = models.ForeignKey(
        ComputingNode, on_delete=models.CASCADE, related_name='node_commands', null=True
    )
    name = models.CharField(
        max_length=255
    )
    syntax = models.JSONField()

    type = models.CharField(
        max_length=255,
        choices=CommandType.choices, default=CommandType.NODE_COMMAND
    )
    resource_necessity = models.JSONField(
        null=True
    )
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ('node', 'name')


class NodeJob(TimeStampedModel):
    otl_query = models.ForeignKey(
        OtlQuery, on_delete=models.CASCADE
    )

    # translated commands, json array of node commands
    commands = models.JSONField()
    status = models.CharField(
        max_length=255,
        choices=NodeJobStatus.choices, default=NodeJobStatus.PLANNED
    )
    next_job = models.ForeignKey(
        'otl_interpreter.NodeJob', null=True, on_delete=models.SET_NULL,
        related_name='awaited_jobs'
    )

    result_address = models.CharField(
        max_length=255
    )

    class Meta:
        app_label = 'otl_interpreter'

