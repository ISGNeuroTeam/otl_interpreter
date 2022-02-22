import logging
import uuid

from rest.views import APIView
from rest.response import SuccessResponse, ErrorResponse, status
from rest.permissions import IsAuthenticated, AllowAny
from otl_interpreter.otl_job_manager import otl_job_manager, QueryError

log = logging.getLogger('otl_interpreter')


class GetJobResultView(APIView):
    permission_classes = (AllowAny, )
    http_method_names = ['get', ]

    def get(self, request):
        log.info('Get result to otl_interpreter')
        job_id = request.GET.get('job_id')
        if not job_id:
            return ErrorResponse(
                error_message='Missing job_id parameter',
                http_status=status.HTTP_400_BAD_REQUEST
            )
        try:
            urls = otl_job_manager.get_result(uuid.UUID(job_id))
        except QueryError:
            return ErrorResponse(
                error_message='Results are not ready yet',
                http_status=status.HTTP_404_NOT_FOUND
            )
        return SuccessResponse(
            {
                'data_urls': list(urls),
            }
        )
