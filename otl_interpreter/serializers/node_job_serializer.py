from rest_framework import serializers

from otl_interpreter.interpreter_db.enums import ComputingNodeType


class NodeJobSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    computing_node_type = serializers.ChoiceField(choices=ComputingNodeType.choices)
    commands = serializers.ListField()



