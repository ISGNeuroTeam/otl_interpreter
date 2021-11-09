import re
import hashlib
import uuid
from django.db import models
from django.utils.translation import gettext_lazy as _
from mptt.models import MPTTModel, TreeForeignKey
from mixins.models import TimeStampedModel
from .enums import JobStatus, NodeJobStatus, CommandType, NodeType, ResultStorage


class OtlJob(TimeStampedModel):
    query = models.TextField(null=False)
    query_hash = models.BinaryField(
        max_length=255, db_index=True, null=False
    )
    uuid = models.UUIDField(
        default=uuid.uuid4, unique=True, editable=False
    )
    tws = models.DateTimeField()
    twf = models.DateTimeField()

    ttl = models.DurationField(
        'ttl', default=60
    )
    user_guid = models.UUIDField(
         null=False
    )
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
        return query_hash.digest()

    class Meta:
        app_label = 'otl_interpreter'

    @staticmethod
    def _remove_repeat_spaces(query):
        return re.sub(r'\s+', ' ', query)

    class Meta:
        app_label = 'otl_interpreter'


class ComputingNode(models.Model):
    # node type: EEP, Spark, PostProcessing.
    type = models.CharField(
        max_length=255,
        choices=NodeType.choices,
        db_index=True,
    )

    # node guid
    guid = models.UUIDField(unique=True)

    active = models.BooleanField(default=True)

    def __str__(self):
        return f'{self.type}: {self.guid}'

    class Meta:
        app_label = 'otl_interpreter'


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

    def __str__(self):
        return f'{self.type} {self.node.guid} {self.name}'

    class Meta:
        unique_together = ('node', 'name')
        app_label = 'otl_interpreter'


class NodeJobResult(models.Model):
    storage = models.CharField(
        'Result storage',
        max_length=255,
        choices=ResultStorage.choices
    )

    # hash of nodejob command tree
    path = models.CharField(
        'path',
        max_length=255,
    )

    # flag to indicate that dataframe was read by client or next job
    was_read = models.BooleanField(default=False)

    # job was finished and calculated
    calculated = models.BooleanField(default=False)

    ttl = models.DurationField(
        default=60
    )
    # last timestamp when nodejob result was read by anyone
    last_touched_timestamp = models.DateTimeField(
        verbose_name=_('Last touched'),
        null=True
    )

    finish_timestamp = models.DateTimeField(
        verbose_name=_('Finish timestamp'),
        null=True
    )

    class Meta:
        app_label = 'otl_interpreter'


class NodeJob(TimeStampedModel, MPTTModel):
    otl_job = models.ForeignKey(
        OtlJob, on_delete=models.CASCADE
    )

    computing_node_type = models.CharField(
        'Node type',
        max_length=255,
        choices=NodeType.choices
    )
    # translated commands, json array of node commands
    commands = models.JSONField()
    status = models.CharField(
        max_length=255,
        choices=NodeJobStatus.choices, default=NodeJobStatus.PLANNED
    )
    next_job = TreeForeignKey(
        'self', null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='awaited_jobs'
    )

    result = models.OneToOneField(
        NodeJobResult, on_delete=models.CASCADE, null=True
    )

    class Meta:
        app_label = 'otl_interpreter'

    class MPTTMeta:
        parent_attr = 'next_job'


