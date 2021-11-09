from rest.urls import path

from .views import MakeJobView, CheckJobView, GetJobResultView, CancelJobView, ScheduleJob


urlpatterns = [
    path(r'makejob/', MakeJobView.as_view()),
    path(r'checkjob/', CheckJobView.as_view()),
    path(r'getresult/', GetJobResultView.as_view()),
    path(r'canceljob/', CancelJobView.as_view()),

    path(r'schedulejob/', ScheduleJob.as_view())
]
