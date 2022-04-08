from django.urls import re_path
from ot_simple_rest_job_proxy.views import makejob, checkjob, getresult

urlpatterns = [
    re_path(r'^makejob/?$', makejob),
    re_path(r'^checkjob/?$', checkjob),
    re_path(r'^getresult/?$', getresult),
]

