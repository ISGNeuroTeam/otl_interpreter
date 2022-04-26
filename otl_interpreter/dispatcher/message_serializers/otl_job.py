from rest_framework import serializers
from django.db.models import TextChoices
from otl_interpreter.interpreter_db.enums import ComputingNodeType, NodeJobStatus, ResultStorage, ResultStatus


class OtlJobCommandName(TextChoices):
    NEW_OTL_JOB = 'NEW_OTL_JOB'
    CANCEL_OTL_JOB = 'CANCEL_JOB'


class OtlJobCommand(serializers.Serializer):
    command_name = serializers.ChoiceField(choices=OtlJobCommandName.choices)
    command = serializers.DictField()


class NodeJobSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    status =  serializers.ChoiceField(choices=NodeJobStatus.choices)
    computing_node_type = serializers.CharField()
    commands = serializers.ListField()
    storage = serializers.ChoiceField(choices=ResultStorage.choices)
    path = serializers.CharField()


class NewOtlJobCommand(serializers.Serializer):
    node_jobs = NodeJobSerializer(many=True)


class CancelOtlJobCommand(serializers.Serializer):
    node_jobs = NodeJobSerializer(many=True)


