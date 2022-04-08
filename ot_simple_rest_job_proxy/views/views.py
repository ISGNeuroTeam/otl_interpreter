import logging
import jwt

from jwt.exceptions import PyJWTError
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import get_user_model
from rest.response import ErrorResponse, status
from ot_simple_rest_job_proxy.settings import ini_config
from ot_simple_rest_job_proxy.job_proxy_manager import job_proxy_manager

from .proxy import proxy_view
from ot_simple_rest_job_proxy.settings import ini_config


User = get_user_model()

# you can use default logger for plugin
log = logging.getLogger('ot_simple_rest_job_proxy')

ot_simple_rest_url = ini_config['ot_simple_rest']['url']
makejob_uri = ot_simple_rest_url + '/' + ini_config['ot_simple_rest']['makejob_urn']

secret_key = ini_config['ot_simple_rest']['secret_key']


def decode_token(token):
    return jwt.decode(token, secret_key, algorithms='HS256')


def get_or_create_user(username):
    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        user = User(username=username)
        user.save()
    return user


@csrf_exempt
def makejob(request):
    log.debug(str(request.COOKIES))
    log.info(f'Get makejob request')

    eva_token = request.COOKIES.get('eva_token')
    if not eva_token:
        return ErrorResponse(error_message='Unauthorized', http_status=status.HTTP_401_UNAUTHORIZED)
    try:
        token_payload = decode_token(eva_token)
    except PyJWTError:
        return ErrorResponse(error_message='Unauthorized', http_status=status.HTTP_401_UNAUTHORIZED)

    username = token_payload['username']
    user = get_or_create_user(username)
    log.warning(str(user.guid))
    log.warning(str(token_payload))
    return proxy_view(request, makejob_uri)


@csrf_exempt
def checkjob(request):
    log.debug(str(request.COOKIES))
    log.info(f'Get checkjob request')
    ot_simple_rest_url = ini_config['ot_simple_rest']['url']
    checkjob_urn = ini_config['ot_simple_rest']['checkjob_urn']
    uri = ot_simple_rest_url + '/' + checkjob_urn
    return proxy_view(request, uri)


@csrf_exempt
def getresult(request):
    log.debug(str(request.COOKIES))
    log.info(f'Get getresult request')
    ot_simple_rest_url = ini_config['ot_simple_rest']['url']
    getresult_urn = ini_config['ot_simple_rest']['getresult_urn']
    uri = ot_simple_rest_url + '/' + getresult_urn
    return proxy_view(request, uri)
