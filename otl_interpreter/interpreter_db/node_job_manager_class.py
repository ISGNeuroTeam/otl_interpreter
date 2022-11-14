import datetime
import logging

from datetime import timedelta
from django.db.models import F
from django.db import transaction
from .models import NodeJob, NodeJobResult, OtlJob, ComputingNode
from .enums import NodeJobStatus, END_STATUSES, ResultStatus, ResultStorage


log = logging.getLogger('otl_interpreter.interpreter_db')


class NodeJobManager:
    def __init__(self, default_cache_ttl):
        self.default_cache_ttl = default_cache_ttl

    def create_node_jobs(self, root_job_tree, otl_job_uuid, otl_job_cache_ttl, node_job_cache_ttl=None, ):
        """
        Creates in database NodeJob tree
        :param root_job_tree: job planer NodeJobTree
        :param otl_job_uuid: otl job uuid
        :param otl_job_cache_ttl: time to store root node job results
        :param node_job_cache_ttl: time to store node job results
        :return:
        """
        node_job_cache_ttl = max(node_job_cache_ttl or self.default_cache_ttl, 30)

        otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
        node_job_for_job_tree = {}

        for node_job_tree in root_job_tree.parent_first_order_traverse_iterator():

            if node_job_tree.parent:
                parent_node_job = node_job_for_job_tree[node_job_tree.parent]
                if node_job_tree.cache_ttl:
                    cache_ttl = node_job_tree.cache_ttl
                else:
                    cache_ttl = node_job_cache_ttl
            else:
                parent_node_job = None
                cache_ttl = otl_job_cache_ttl

            result_address = node_job_tree.result_address
            if result_address:
                node_job_result = NodeJobManager._find_or_create_node_job_result(
                    result_address.path,
                    result_address.storage_type,
                    cache_ttl
                )
                # mark result is calculated
                if node_job_result.status == ResultStatus.CALCULATED:
                    node_job_tree.result_calculated = True
            else:
                node_job_result = None

            node_job = NodeJob(
                otl_job=otl_job,
                uuid=node_job_tree.uuid,
                computing_node_type=node_job_tree.computing_node_type,
                commands=node_job_tree.as_command_dict_list(),
                next_job=parent_node_job,
                result=node_job_result,
            )
            node_job.save()
            node_job_for_job_tree[node_job_tree] = node_job

    @staticmethod
    def _find_or_create_node_job_result(path, storage_type, cache_ttl):
        """
        If such result already exists returns it or create a new one
        """
        try:
            node_job_result = NodeJobResult.objects.get(
                path=path, storage=storage_type
            )
            # not allow new query reduce ttl of old query
            node_job_result.ttl = timedelta(seconds=max(node_job_result.ttl.seconds, cache_ttl))
            log.info(f'Find node job with same result, it is in {node_job_result.status} state')

        except NodeJobResult.DoesNotExist:
            node_job_result = NodeJobResult(
                storage=storage_type,
                path=path,
                ttl=timedelta(seconds=cache_ttl),
            )
        node_job_result.last_touched_timestamp = datetime.datetime.now()
        node_job_result.save()
        return node_job_result

    @staticmethod
    def set_computing_node_for_node_job(node_job_uuid,  computing_node_uuid=None):
        try:
            node_job = NodeJob.objects.get(uuid=node_job_uuid)
            if computing_node_uuid is not None:
                computing_node = ComputingNode.objects.get(uuid=computing_node_uuid)
                node_job.computing_node = computing_node
            else:
                node_job.computing_node = None

            node_job.save()
        except NodeJob.DoesNotExist:
            log.error(f'Setting computing node for unexisting nodejob: {node_job_uuid}')
        except ComputingNode.DoesNotExist:
            log.error(f'Setting unexisting computing node for nodejob: {computing_node_uuid}')

    @staticmethod
    def get_computing_node_for_node_job(node_job):
        """
        Returns computing node uuid for node job
        :param node_job: node job uuid or NodeJob instance
        :return:
        """
        if not isinstance(node_job, NodeJob):
            node_job = NodeJob.objects.get(uuid=node_job)

        return node_job.computing_node.uuid

    @staticmethod
    def get_node_job_status(node_job_uuid):
        """
        Returns computing node job status
        :param node_job_uuid: node job uuid
        """
        try:
            node_job = NodeJob.objects.get(uuid=node_job_uuid)
            return node_job.status
        except NodeJob.DoesNotExist:
            log.error(f'Get node job status for unexisting nodejob: {node_job_uuid}')
        return None

    @staticmethod
    def change_node_job_status(node_job_uuid, status: NodeJobStatus, status_text: str = None):
        try:
            node_job = NodeJob.objects.get(uuid=node_job_uuid)
            node_job.status = status
            node_job.status_text = status_text
            node_job.save()

        except NodeJob.DoesNotExist:
            log.error(f'Setting node job status for unexisting nodejob: {node_job_uuid}')

    @staticmethod
    def get_running_node_job_uuids_for_computing_node(computing_node_uuid):
        """
        Returns list of node job uuids running on computing node
        """
        return list(NodeJob.objects.filter(
            status=NodeJobStatus.RUNNING, computing_node__uuid=computing_node_uuid
        ).values_list('uuid', flat=True))

    def get_next_node_job_to_execute(self, finished_node_uuid):
        """
        Returns serialized node_job ready to be executed or None
        :param finished_node_uuid: recently finished node job uuid
        :return:
        If next node job exists and ready to be executed returns dictionary representing node job
        Returns None otherwise
        """
        finished_node_job = NodeJob.objects.get(uuid=finished_node_uuid)
        next_node_job = finished_node_job.next_job
        if next_node_job:
            # check that doesn't exist unfinished node jobs
            if NodeJob.objects.filter(
                next_job=next_node_job).exclude(
                status=NodeJobStatus.FINISHED
            ).exists():
                return None
            else:
                return self.get_node_job_dict(node_job=next_node_job)
        return None

    def is_root_node_job(self, node_job_uuid):
        node_job = NodeJob.objects.get(uuid=node_job_uuid)
        return node_job.level == 0

    @staticmethod
    def get_otl_job_uuid(node_job):
        if not isinstance(node_job, NodeJob):
            node_job = NodeJob.objects.get(uuid=node_job)
        return node_job.otl_job.uuid

    @staticmethod
    def get_node_job_dict(node_job):
        """
        returns node job dictionary representation or None if node job does't exist
        :param node_job: NodeJob instance or node job uuid
        :param node_job: NodeJob instance
        :return:
        """
        try:
            if not isinstance(node_job, NodeJob):
                node_job = NodeJob.objects.get(uuid=node_job)
        except NodeJob.DoesNotExist:
            log.error(f'Node job with uuid={str(node_job)} does not exist')
            return None

        return {
            'uuid': node_job.uuid,
            'status': node_job.status,
            'computing_node_type': node_job.computing_node_type,
            'commands': node_job.commands,
            'storage': node_job.result.storage,
            'path': node_job.result.path
        }

    @staticmethod
    def get_result_status(storage: ResultStorage, path):
        """
        Returns status of node job result
        """
        try:
            result = NodeJobResult.objects.get(storage=storage, path=path)
            result.last_touched_timestamp = datetime.datetime.now()
            result.save()
            return result.status
        except NodeJobResult.DoesNotExist:
            return ResultStatus.NOT_EXIST

    @staticmethod
    def set_not_exist_status_for_expired_results():
        """
        Find results that expired and set status to NOT_EXIST
        Returns list of tuples with storage and path
        """
        with transaction.atomic():
            results = NodeJobResult.objects.filter(
                status=ResultStatus.CALCULATED,
                last_touched_timestamp__lt=datetime.datetime.now()-F('ttl')
            )
            result_list = [(r[0], r[1]) for r in results.values_list('storage', 'path')]
            results = results.select_for_update()
            results.update(status=ResultStatus.NOT_EXIST)

        return result_list

    @staticmethod
    def set_result_status(storage: ResultStorage, path, status: ResultStatus):
        """
        Sets status result
        """
        try:
            result = NodeJobResult.objects.get(storage=storage, path=path)
            result.status = status
            result.last_touched_timestamp = datetime.datetime.now()
            if status == ResultStatus.CALCULATED:
                result.finish_timestamp = datetime.datetime.now()
            result.save()
        except NodeJobResult.DoesNotExist:
            log.error(f'Can\'t find node job result with storage = {storage}, path={path}')

    @staticmethod
    def get_waiting_same_result_node_jobs(storage: ResultStorage, path):
        """
        Returns dict of node jobs waiting the same result
        """
        try:
            result = NodeJobResult.objects.get(storage=storage, path=path)
            return list(map(
                NodeJobManager.get_node_job_dict,
                NodeJob.objects.filter(result=result, status=NodeJobStatus.WAITING_SAME_RESULT)
            ))
        except NodeJobResult.DoesNotExist:
            return []

    @staticmethod
    def get_unfinished_node_jobs_for_otl_job(otl_job_uuid):
        """
        Returns list node jobs dictionary for otl job
        """
        try:
            otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
            node_jobs = NodeJob.objects.filter(otl_job=otl_job).exclude(status__in=END_STATUSES)
            return list(
                map(
                    lambda node_job: NodeJobManager.get_node_job_dict(node_job),
                    node_jobs
                ),
            )
        except OtlJob.DoesNotExist:
            log.error(f'Can\'t find otl job with uuid {otl_job_uuid}')

        return []

    @staticmethod
    def get_node_jobs_for_cancelling(failed_node_job):
        """
        Get running node jobs uuid that must be cancel
        :failed_node_job_uuid: failed node job
        :return:
        list of node jobs uuids that in running state
        """
        if not isinstance(failed_node_job, NodeJob):
            failed_node_job = NodeJob.objects.get(uuid=failed_node_job)

        running_node_jobs = NodeJob.objects.filter(
            otl_job=failed_node_job.otl_job
        ).exclude(status__in=END_STATUSES)

        return list(running_node_jobs.values_list('uuid', flat=True))

    @staticmethod
    def delete_old_node_job_results(older_than: datetime.timedelta):
        """
        Delete all node job results older than given timedelta
        """
        old_node_jobs = NodeJobResult.objects.filter(last_touched_timestamp__lt=datetime.datetime.now() - older_than)
        old_node_jobs.delete()
