from rest_framework import serializers
from otl_interpreter.interpreter_db.enums import ComputingNodeType


class ComputingNodeRegisterSerializer(serializers.Serializer):
    uuid = serializers.UUIDField()
    type = serializers.ChoiceField(
        choices=ComputingNodeType.choices
    )
