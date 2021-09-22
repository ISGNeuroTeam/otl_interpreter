from rest.urls import path

from .views import CreateJobView, CheckJobView, GetJobResultView, CancelJobView

urlpatterns = [
    path(r'makejob/', CreateJobView.as_view()),
    path(r'checkjob/', CheckJobView.as_view()),
    path(r'getresult/', GetJobResultView.as_view()),
    path(r'canceljob/', CancelJobView.as_view()),
]