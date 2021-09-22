import logging

from rest.views import APIView
from rest.response import Response, status
from rest.permissions import IsAuthenticated, AllowAny

from otl_interpreter.settings import ini_config
log = logging.getLogger('otl_interpreter')


class GetJobResultView(APIView):
    permission_classes = (AllowAny, )
    http_method_names = ['get', ]

    def get(self, request):
        log.info('Get result to otl_interpreter')
        only_address = 'only_address' in request.GET
        return Response(
            {
                'status': 'ok',
                'only_address': bool(only_address)
            },
            status.HTTP_200_OK
        )



