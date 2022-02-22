import logging

from uuid import UUID

from rest.views import APIView
from rest.response import SuccessResponse, ErrorResponse, status
from rest.permissions import IsAuthenticated
from otl_interpreter.otl_job_manager import otl_job_manager


log = logging.getLogger('otl_interpreter')


class CheckJobView(APIView):
    permission_classes = (IsAuthenticated, )
    http_method_names = ['get', ]

    def get(self, request):
        job_id = request.GET.get('job_id')
        try:
            job_id: UUID = UUID(job_id)
        except (ValueError, TypeError) as err:
            return ErrorResponse(
                error_message=str(err),
                http_status=status.HTTP_400_BAD_REQUEST
            )

        job_status, job_status_text = otl_job_manager.check_job(job_id)

        return SuccessResponse(
            {
                'job_status': job_status,
                'job_status_text': job_status_text
            },
        )



