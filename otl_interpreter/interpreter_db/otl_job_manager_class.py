import datetime

from uuid import UUID

from .models import OtlJob
from .enums import JobStatus


class OtlJobManager:
    def make_otl_job(self, otl_query, user_guid, tws, twf, ttl):
        """
        Creates new OtlQuery instance in database 
        """
        otl_job = OtlJob(
            query=otl_query, user_guid=user_guid,
            tws=tws, twf=twf, ttl=datetime.timedelta(seconds=ttl), status=JobStatus.NEW
        )
        otl_job.save()
        return otl_job.uuid

    def check_job(self, otl_job_id: UUID):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        return otl_job.status, otl_job.status_text

    def cancel_job(self, otl_job_id: UUID):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        otl_job.status = JobStatus.CANCELED
        otl_job.save()
        return otl_job.status



