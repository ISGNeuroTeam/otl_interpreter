from rest_framework import serializers
from otl_interpreter.interpreter_db.enums import NodeJobStatus


class NodeJobStatusSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    status = serializers.ChoiceField(choices=NodeJobStatus.choices)
    status_text = serializers.CharField(default=None, allow_null=True)
    last_finished_command = serializers.CharField(
        default=None, allow_null=True
    )
