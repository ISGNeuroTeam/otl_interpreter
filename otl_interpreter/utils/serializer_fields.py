import datetime

from uuid import UUID
from rest_framework import serializers


class TimestampField(serializers.Field):

    def to_internal_value(self, data):
        """
        Transform the *incoming* primitive data into a native value.
        """
        return datetime.datetime.fromtimestamp(int(data))

    def to_representation(self, value):
        """
        Transform the *outgoing* native value into primitive data.
        """
        return int(value.timestamp())

