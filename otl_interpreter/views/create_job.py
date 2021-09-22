import logging

from rest.views import APIView
from rest.response import Response, status, APIException
from rest.permissions import IsAuthenticated, AllowAny

from ..translator import translate

from otl_interpreter.settings import ini_config
log = logging.getLogger('otl_interpreter')


class CreateJobView(APIView):
    permission_classes = (AllowAny, )
    http_method_names = ['post', ]

    def post(self, request):
        if 'otl_query' not in request.data:
            raise APIException('otl_query field required', code=status.HTTP_400_BAD_REQUEST)

        log.info('Make job to otl_interpreter')
        return Response(
            {
                'status': 'ok',
                'otl_query': request.data['otl_query']
            },
            status.HTTP_200_OK
        )



