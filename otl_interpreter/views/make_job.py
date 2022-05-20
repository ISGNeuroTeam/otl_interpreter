import logging

from rest.views import APIView
from rest.response import Response, status, APIException, SuccessResponse, ErrorResponse
from rest.permissions import IsAuthenticated, AllowAny

from .serializers import MakeJobSerislizer

from otl_interpreter.otl_job_manager import otl_job_manager, QueryError

log = logging.getLogger('otl_interpreter')


class MakeJobView(APIView):
    permission_classes = (IsAuthenticated,)
    http_method_names = ['post', ]

    def post(self, request):
        make_job_serializer = MakeJobSerislizer(data=request.data)
        make_job_serializer.is_valid(raise_exception=True)
        try:
            job_id, storage_type, path = otl_job_manager.makejob(
                make_job_serializer.validated_data['otl_query'],
                request.user.guid,
                tws=make_job_serializer.validated_data['tws'],
                twf=make_job_serializer.validated_data['twf'],
                otl_job_cache_ttl=make_job_serializer.validated_data['cache_ttl'],
                timeout=make_job_serializer.validated_data['timeout'],
                shared_post_processing=make_job_serializer.validated_data['shared_post_processing'],
                subsearch_is_node_job=make_job_serializer.validated_data['subsearch_is_node_job'],
            )
        except QueryError as err:
            return ErrorResponse(
                error_message=str(err)
            )

        return SuccessResponse(
            {
                'job_id': job_id.hex,
                'storage_type': storage_type,
                'path': path,
            }
        )
