import logging

from rest.views import APIView
from rest.response import Response, status
from rest.permissions import IsAuthenticated, AllowAny

from otl_interpreter.settings import ini_config
log = logging.getLogger('otl_interpreter')


class CancelJobView(APIView):
    permission_classes = (AllowAny, )
    http_method_names = ['post', ]

    def post(self, request):
        log.info('Cancel to otl_interpreter')
        return Response(
            {
                'status': 'ok',
            },
            status.HTTP_200_OK
        )



