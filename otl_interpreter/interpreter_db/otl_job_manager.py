import datetime

from .models import OtlJob
from .enums import JobStatus


class OtlJobManager:
    def make_otl_job(self, otl_query, user_guid, tws, twf, ttl):
        """
        Creates new OtlQuery instance in database 
        """
        otl_job = OtlJob(
            query=otl_query, user_guid=user_guid,
            tws=tws, twf=twf, ttl=datetime.timedelta(ttl), status=JobStatus.NEW
        )
        otl_job.save()
        return otl_job.uuid

    def check_job(self, otl_job_hash):
        pass

