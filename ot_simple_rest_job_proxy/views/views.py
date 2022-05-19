import logging
import jwt
import json

from io import BytesIO
from jwt.exceptions import PyJWTError

from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import get_user_model
from django.http import HttpResponse, QueryDict

from rest.response import ErrorResponse, Response, status

from ot_simple_rest_job_proxy.job_proxy_manager import job_proxy_manager
from ot_simple_rest_job_proxy.settings import ini_config

from .proxy import proxy_view

User = get_user_model()

# you can use default logger for plugin
log = logging.getLogger('ot_simple_rest_job_proxy')

ot_simple_rest_url = ini_config['ot_simple_rest']['url']

makejob_uri = ot_simple_rest_url + '/' + ini_config['ot_simple_rest']['makejob_urn']
checkjob_uri = ot_simple_rest_url + '/' + ini_config['ot_simple_rest']['checkjob_urn']
getresult_uri = ot_simple_rest_url + '/' + ini_config['ot_simple_rest']['getresult_urn']

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
    # django forbids access to body property after reading POST,
    # so to get post data without error in proxy_view we are parsing body
    data = BytesIO(request.body)
    if request.content_type == 'application/x-www-form-urlencoded':
        post_dict = QueryDict(request._body, encoding=request._encoding)
    elif request.content_type == 'multipart/form-data':
        post_dict, _ = request.parse_file_upload(request.META, data)
    else:
        post_dict = QueryDict(encoding=request._encoding)

    query = post_dict['original_otl']
    tws = post_dict['tws']
    twf = post_dict['twf']
    cache_ttl = post_dict['cache_ttl']

    new_platform_query_index = job_proxy_manager.is_new_platform_query(query)

    if not new_platform_query_index:
        return proxy_view(request, makejob_uri)

    log.info(f'Get makejob request for new platform{query}, tws={tws}, twf={twf}, cache_ttl={cache_ttl}')

    eva_token = request.COOKIES.get('eva_token')
    if not eva_token:
        return ErrorResponse(error_message='Unauthorized', http_status=status.HTTP_401_UNAUTHORIZED)
    try:
        token_payload = decode_token(eva_token)
    except PyJWTError:
        return ErrorResponse(error_message='Unauthorized', http_status=status.HTTP_401_UNAUTHORIZED)

    username = token_payload['username']
    user = get_or_create_user(username)
    resp = job_proxy_manager.makejob(query[new_platform_query_index:], user.guid, tws, twf, cache_ttl)
    return HttpResponse(json.dumps(resp))


@csrf_exempt
def checkjob(request):
    query = request.GET['original_otl']
    new_platform_query_index = job_proxy_manager.is_new_platform_query(query)

    if not new_platform_query_index:
        return proxy_view(request, checkjob_uri)

    tws = request.GET['tws']
    twf = request.GET['twf']

    log.info(f'Get checkjob request for new platform{query}, tws={tws}, twf={twf}')
    resp = job_proxy_manager.checkjob(query[new_platform_query_index:], tws, twf)
    return HttpResponse(json.dumps(resp))


@csrf_exempt
def getresult(request):
    cid = request.GET['cid']
    if not job_proxy_manager.is_new_platform_query_id(cid):
        return proxy_view(request, getresult_uri)

    log.info(f'Get getresult request with cid={cid}')

    resp = job_proxy_manager.getresult(cid)
    return HttpResponse(json.dumps(resp))
