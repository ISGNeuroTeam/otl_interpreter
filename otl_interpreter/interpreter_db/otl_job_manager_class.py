import logging
import datetime

from typing import Optional
from uuid import UUID
from django.db.models import F
from django.core.exceptions import ObjectDoesNotExist
from .models import OtlJob, NodeJob, NodeJobResult
from .enums import JobStatus, NodeJobStatus

log = logging.getLogger('otl_interpreter.interpreter_db')


class OtlJobManager:
    def make_otl_job(self, otl_query, user_guid, tws, twf, ttl, timeout):
        """
        Creates new OtlQuery instance in database 
        """
        otl_job = OtlJob(
            query=otl_query, user_guid=user_guid,
            tws=tws, twf=twf, ttl=datetime.timedelta(seconds=ttl),
            status=JobStatus.NEW, timeout=datetime.timedelta(seconds=timeout)
        )
        otl_job.save()
        return otl_job.uuid

    def check_job(self, otl_job_id: UUID):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        return otl_job.status, otl_job.status_text

    def get_result(self, otl_job_id: UUID) -> Optional[NodeJobResult]:
        try:
            otl_job = OtlJob.objects.get(uuid=otl_job_id)
            root_job = NodeJob.objects.get(otl_job=otl_job, next_job=None)
        except ObjectDoesNotExist as err:
            log.warning(f'Otl job with uuid = {str(otl_job_id)} not found in database. \n{str(err)}')
            return None
        return root_job.result

    def cancel_job(self, otl_job_id: UUID, status_text=None):
        otl_job = OtlJob.objects.get(uuid=otl_job_id)
        otl_job.status = JobStatus.CANCELED
        otl_job.status_text = status_text or 'Canceled by user'
        otl_job.save()
        return otl_job.status

    @staticmethod
    def change_otl_job_status(otl_job_uuid: UUID, status, status_text=None):
        try:
            otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
            if status == JobStatus.RUNNING:
                status_text = (status_text if status_text is not None else '') +\
                              OtlJobManager._form_running_status_message(otl_job)

            if status == JobStatus.FAILED:
                status_text = (status_text if status_text is not None else '') + \
                                OtlJobManager._form_fail_status_message(otl_job)
                log.error(f'Failed and status_text={status_text}')
            otl_job.status = status
            otl_job.status_text = status_text
            otl_job.save()
        except OtlJob.DoesNotExist:
            log.error(f'Otl job with uuid: {otl_job_uuid} doesn\'t exist')

    @staticmethod
    def _form_fail_status_message(otl_job: OtlJob):
        failed_node_jobs = otl_job.nodejobs.filter(status=NodeJobStatus.FAILED)
        status_text = '\n'.join(map(
            lambda node_job: str(node_job.uuid.hex) + ': ' + node_job.status_text,
            failed_node_jobs
        ))
        return status_text

    @staticmethod
    def _form_running_status_message(otl_job: OtlJob):
        total_node_jobs = otl_job.nodejobs.count()
        running_node_jobs = otl_job.nodejobs.filter(status=NodeJobStatus.RUNNING).count()
        finished_node_jobs = otl_job.nodejobs.filter(status=NodeJobStatus.FINISHED).count()
        return f'Running {running_node_jobs} of {total_node_jobs} node_jobs. Finished {finished_node_jobs} '

    @staticmethod
    def delete_old_otl_query_info(older_than: datetime.timedelta):
        """
        Delete from database all otl queries that created before than <now - given timedelta>
        Returns job id list
        """
        old_otl_queries = OtlJob.objects.filter(created_time__lt=datetime.datetime.now()-older_than)
        uuids = list(old_otl_queries.values_list('uuid', flat=True))
        old_otl_queries.delete()
        return uuids

    @staticmethod
    def get_otl_jobs_with_expired_timeout():
        """
        Finds running otl jobs with expired timeout
        Returns list of otl job uuids
        """
        jobs_with_expired_timeout = OtlJob.objects.filter(
            status__in=(JobStatus.RUNNING, JobStatus.PLANNED)
        ).exclude(
            timeout=datetime.timedelta(seconds=0)
        ).filter(created_time__lt=datetime.datetime.now() - F('timeout')).values_list('uuid', flat=True)
        return list(jobs_with_expired_timeout)

