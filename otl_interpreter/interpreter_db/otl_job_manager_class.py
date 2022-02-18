import logging
import datetime

from uuid import UUID

from .models import OtlJob, NodeJob
from .enums import JobStatus

log = logging.getLogger('otl_interpreter.interpreter_db')


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

    def get_result(self, otl_job_id: UUID):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        root_job = NodeJob.objects.get(otl_job=otl_job, next_job=None)
        return root_job.result

    def cancel_job(self, otl_job_id: UUID):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        otl_job.status = JobStatus.CANCELED
        otl_job.save()
        return otl_job.status

    @staticmethod
    def change_otl_job_status(otl_job_uuid: UUID, status, status_text=None):
        try:
            otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
            otl_job.status = status
            otl_job.status_text = status_text
            otl_job.save()
        except OtlJob.DoesNotExist:
            log.error(f'Otl job with uuid: {otl_job_uuid} doesn\'t exist')




