import datetime
import logging

from datetime import timedelta

from otl_interpreter.interpreter_db.models import NodeJob, NodeJobResult, OtlJob, ComputingNode
from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus


log = logging.getLogger('otl_interpreter.interpreter_db')


class NodeJobManager:
    def __init__(self, default_cache_ttl):
        self.default_cache_ttl = default_cache_ttl

    def create_node_jobs(self, root_job_tree, otl_job_uuid, cache_ttl=None):
        """
        Creates in database NodeJob tree
        :param root_job_tree: job planer NodeJobTree
        :param otl_job_uuid: otl job uuid
        :param cache_ttl: time to store node job results
        :return:
        """
        cache_ttl = cache_ttl or self.default_cache_ttl
        otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
        node_job_for_job_tree = {}

        for node_job_tree in root_job_tree.parent_first_order_traverse_iterator():

            if node_job_tree.parent:
                parent_node_job = node_job_for_job_tree[node_job_tree.parent]
            else:
                parent_node_job = None

            result_address = node_job_tree.result_address
            if result_address:
                node_job_result = NodeJobResult(
                    storage=result_address.storage_type,
                    path=result_address.path,
                    ttl=timedelta(cache_ttl),
                )
                node_job_result.save()
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
    def set_computing_node_for_node_job(node_job_uuid,  computing_node_uuid):
        try:
            node_job = NodeJob.objects.get(uuid=node_job_uuid)
            computing_node = ComputingNode.objects.get(uuid=computing_node_uuid)
            node_job.computing_node = computing_node
            node_job.save()
        except NodeJob.DoesNotExist:
            log.error(f'Setting computing node for unexisting nodejob: {node_job_uuid}')
        except ComputingNode.DoesNotExist:
            log.error(f'Setting unexisting computing node for nodejob: {computing_node_uuid}')

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
            log.error(f'Get node job status for unexisting nodejob: {node_job_uuid} {str(err)}')
        return None

    @staticmethod
    def change_node_job_status(node_job_uuid, status: NodeJobStatus, status_text: str = None):
        try:
            node_job = NodeJob.objects.get(uuid=node_job_uuid)
            node_job.status = status
            node_job.status_text = status_text
            if status == NodeJobStatus.FINISHED:
                node_job.result.finish_timestamp = datetime.datetime.now()
                node_job.result.last_touched_timestamp = datetime.datetime.now()
                node_job.result.calculated = True
                node_job.result.save()
            node_job.save()

        except NodeJob.DoesNotExist:
            log.error(f'Setting node job status for unexisting nodejob: {uuid}')

    @staticmethod
    def get_computing_nodes_and_node_jobs_uuids_for_cancel(failed_node_job_uuid):
        """
        When one node job failed, other nodes jobs need to be canceled
        :param failed_node_job_uuid:
        :return:
        """
        pass

    @staticmethod
    def get_next_node_job_to_execute_or_finish_otl(finished_node_uuid):
        """
        Returns serialized node_job ready to be executed or None
        Checks if all node jobs finished. If true finish otl job

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
                return {
                    'uuid': next_node_job.uuid,
                    'computing_node_type': next_node_job.computing_node_type,
                    'commands': next_node_job.commands
                }
        else:
            otl_job = finished_node_job.otl_job
            otl_job.status = 'FINISHED'
            otl_job.save()

        return None

    @staticmethod
    def get_node_job_dict(node_job_uuid):
        node_job = NodeJob.objects.get(uuid=node_job_uuid)
        return {
            'uuid': node_job.uuid,
            'computing_node_type': node_job.computin_node_type,
            'commands': node_job.commands
        }



