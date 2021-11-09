import logging

from django_celery_beat.models import CrontabSchedule

from rest.views import APIView
from rest.response import Response, status, APIException
from rest.permissions import IsAuthenticated, AllowAny



from otl_interpreter.settings import ini_config
log = logging.getLogger('otl_interpreter')


class ScheduleJob(APIView):
    permission_classes = (IsAuthenticated, )
    http_method_names = ['post', ]

    def post(self, request):

        log.debug('Otl query scheduled')

        return Response(
            {
                'status': 'ok',
                'otl_query': request.data['otl_query']
            },
            status.HTTP_200_OK
        )



