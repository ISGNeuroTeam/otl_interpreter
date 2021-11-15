import logging

from uuid import UUID

from rest.views import APIView
from rest.response import SuccessResponse
from rest.permissions import IsAuthenticated
from otl_interpreter.otl_job_manager import otl_job_manager

from .serializers import CheckJobSerializer

log = logging.getLogger('otl_interpreter')


class CheckJobView(APIView):
    permission_classes = (IsAuthenticated, )
    http_method_names = ['get', ]

    def get(self, request):

        check_job_serializer = CheckJobSerializer(data=request.data)
        check_job_serializer.is_valid(raise_exception=True)

        job_id: UUID = check_job_serializer.validated_data['job_id']

        job_status, job_status_text = otl_job_manager.check_job(job_id)

        return SuccessResponse(
            {
                'job_status': job_status,
                'job_status_text': job_status_text
            },
        )



