from rest_framework import serializers
from django.db.models import TextChoices
from otl_interpreter.interpreter_db.enums import ComputingNodeType


class OtlJobCommandName(TextChoices):
    NEW_OTL_JOB = 'NEW_OTL_JOB'
    CANCEL_OTL_JOB = 'CANCEL_JOB'


class OtlJobCommand(serializers.Serializer):
    command_name = serializers.ChoiceField(choices=OtlJobCommandName.choices)
    command = serializers.DictField()


class NodeJobSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    computing_node_type = serializers.ChoiceField(choices=ComputingNodeType.choices)
    commands = serializers.ListField()


class NewOtlJobCommand(serializers.Serializer):
    node_jobs = NodeJobSerializer(many=True)


class CancelOtlJobCommand(serializers.Serializer):
    uuid = serializers.UUIDField()


