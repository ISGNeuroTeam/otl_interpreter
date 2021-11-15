from rest_framework import serializers
from otl_interpreter.utils.serializer_fields import TimestampField


class MakeJobSerislizer(serializers.Serializer):
    otl_query = serializers.CharField()
    tws = TimestampField()
    twf = TimestampField()
    cache_ttl = serializers.IntegerField(min_value=0, default=None, allow_null=True)
    timeout = serializers.IntegerField(min_value=0, default=None, allow_null=True)
    shared_post_processing = serializers.BooleanField(default=None, allow_null=True)
    subsearch_is_node_job = serializers.BooleanField(default=None, allow_null=True)


class CheckJobSerializer(serializers.Serializer):
    job_id = serializers.UUIDField()


class CancelJobSerializer(serializers.Serializer):
    job_id = serializers.UUIDField()

