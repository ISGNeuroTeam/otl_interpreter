from rest_framework import serializers
from django.db.models import TextChoices


class ControlNodeCommandName(TextChoices):
    REGISTER_COMPUTING_NODE = 'REGISTER_COMPUTING_NODE'
    UNREGISTER_COMPUTING_NODE = 'UNREGISTER_COMPUTING_NODE'
    RESOURCE_STATUS = 'RESOURCE_STATUS'
    ERROR_OCCURED = 'ERROR_OCCURED'


class ComputingNodeControlCommand(serializers.Serializer):
    computing_node_uuid = serializers.UUIDField()
    command_name = serializers.ChoiceField(choices=ControlNodeCommandName.choices)
    command = serializers.DictField()


class RegisterComputingNodeCommand(serializers.Serializer):
    computing_node_type = serializers.CharField()
    host_id = serializers.CharField()
    otl_command_syntax = serializers.DictField()
    resources = serializers.DictField()


class UnregisterComputingNodeCommand(serializers.Serializer):
    pass


class ErrorOccuredCommand(serializers.Serializer):
    error = serializers.CharField()


class ResourceStatusCommand(serializers.Serializer):
    resources = serializers.DictField()


