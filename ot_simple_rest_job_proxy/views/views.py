import logging
import jwt
import json

from io import BytesIO
from jwt.exceptions import PyJWTError

from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import get_user_model
from django.http import HttpResponse, QueryDict

from rest.response import ErrorResponse, Response, status
from rest_auth.authentication import JWTAuthentication

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


def post_dict_decor(f):
    """
    Adds post_dict attribute to request
    """
    def wrapper(request, *args, **kwargs):
        # django forbids access to body property after reading POST,
        # so to get post data without error in proxy_view we are parsing body
        data = BytesIO(request.body)
        if request.content_type == 'application/x-www-form-urlencoded':
            post_dict = QueryDict(request._body, encoding=request._encoding)
        elif request.content_type == 'multipart/form-data':
            post_dict, _ = request.parse_file_upload(request.META, data)
        elif request.content_type == 'application/json' or request.content_type == 'text/plain':
            post_dict = json.loads(request._body.decode(request._encoding))
        else:
            post_dict = QueryDict(encoding=request._encoding)
        request.post_dict = post_dict
        return f(request, *args, **kwargs)

    return wrapper


@csrf_exempt
@post_dict_decor
def makejob(request):

    query = request.post_dict['original_otl']
    tws = request.post_dict['tws']
    twf = request.post_dict['twf']
    cache_ttl = request.post_dict['cache_ttl']
    timeout = request.post_dict.get('timeout', '0')

    new_platform_query_index = job_proxy_manager.is_new_platform_query(query)

    if not new_platform_query_index:
        return proxy_view(request, makejob_uri)

    log.info(
        f'Get makejob request for new platform{query}, tws={tws}, twf={twf}, cache_ttl={cache_ttl}, timeout={timeout}'
    )

    # check complex rest auth
    jwt_auth = JWTAuthentication()
    user_token_tuple = jwt_auth.authenticate(request)

    # if complex rest authentication not work try old authentication
    if not user_token_tuple:
        eva_token = request.COOKIES.get('eva_token')
        if not eva_token:
            return HttpResponse(content='Unauthorized', status=status.HTTP_401_UNAUTHORIZED)
        try:
            token_payload = decode_token(eva_token)
        except PyJWTError:
            return HttpResponse(content='Unauthorized', status=status.HTTP_401_UNAUTHORIZED)

        username = token_payload['username']
        user = get_or_create_user(username)
    else:
        user = user_token_tuple[0]

    resp = job_proxy_manager.makejob(query[new_platform_query_index:], user.guid, tws, twf, cache_ttl, timeout)
    return HttpResponse(json.dumps(resp))


@csrf_exempt
@post_dict_decor
def checkjob(request):
    request.post_dict = request.post_dict or request.GET
    query = request.post_dict['original_otl']
    new_platform_query_index = job_proxy_manager.is_new_platform_query(query)

    if not new_platform_query_index:
        return proxy_view(request, checkjob_uri)

    tws = request.post_dict['tws']
    twf = request.post_dict['twf']

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
