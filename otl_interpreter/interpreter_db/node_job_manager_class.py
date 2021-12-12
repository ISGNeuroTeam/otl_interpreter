import datetime
import logging

from datetime import timedelta

from otl_interpreter.interpreter_db.models import NodeJob, NodeJobResult, OtlJob, ComputingNode
from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus


log = logging.getLogger('otl_interpreter.interpreter_db')

# sets of allowed next states
allowed_state_transfer_table = {
    NodeJobStatus.PLANNED: {
        NodeJobStatus.IN_QUEUE, NodeJobStatus.SENT_TO_COMPUTING_NODE, NodeJobStatus.FINISHED, NodeJobStatus.CANCELED
    },
    NodeJobStatus.IN_QUEUE: {
        NodeJobStatus.TAKEN_FROM_QUEUE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.TAKEN_FROM_QUEUE: {
        NodeJobStatus.SENT_TO_COMPUTING_NODE, NodeJobStatus.IN_QUEUE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.SENT_TO_COMPUTING_NODE: {
        NodeJobStatus.RUNNING, NodeJobStatus.DECLINED_BY_COMPUTING_NODE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.DECLINED_BY_COMPUTING_NODE: {
        NodeJobStatus.IN_QUEUE, NodeJobStatus.SENT_TO_COMPUTING_NODE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.RUNNING: {
        NodeJobStatus.FINISHED, NodeJobStatus.FAILED, NodeJobStatus.CANCELED
    },
    NodeJobStatus.FINISHED: {},
    NodeJobStatus.CANCELED: {},
    NodeJobStatus.FAILED: {},
}


def is_next_node_job_status_allowed(cur_status,  next_status):
    return next_status in allowed_state_transfer_table[cur_status]


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

    def change_node_job_status(self, uuid, status: NodeJobStatus, status_text: str = None):
        try:
            node_job = NodeJob.objects.get(uuid=uuid)
            if not is_next_node_job_status_allowed(node_job.status, status):
                log.warning(
                    f'Trying set not allowed node job status. NodeJob uuid: {node_job.uuid},\
                     status: {node_job.status}, next status: {status}'
                )
                return False

            node_job.status = status
            node_job.status_text = status_text

            if status == NodeJobStatus.FINISHED:
                self._finished_status(node_job)

            if status == NodeJobStatus.FAILED:
                self._failed_status(node_job, status_text)

            node_job.save()

        except NodeJob.DoesNotExist:
            log.error(f'Setting node job status for unexisting nodejob: {uuid}')

    @staticmethod
    def _finished_status(node_job):
        # if no next job then otl job is done
        # if node_job.next_job is None:
        #     otl_job_manager.change_otl_job_status(
        #         node_job.otl_job.uuid, JobStatus.FINISHED
        #     )
        # node_job.result.finish_timestamp = datetime.datetime.now()
        # node_job.result.last_touched_timestamp = datetime.datetime.now()
        # node_job.result.calculated = True
        pass

    @staticmethod
    def _failed_status(node_job, status_text):
        # otl_job_manager.change_otl_job_status(
        #     node_job.otl_job.uuid, JobStatus.FAILED,
        #     f'Failed because of error in node job: {node_job.uuid}. Error: {status_text}'
        # )
        pass

    @staticmethod
    def get_computing_nodes_and_node_jobs_uuids_for_cancel(failed_node_job_uuid):
        """
        When one node job failed, other nodes jobs need to be canceled
        :param failed_node_job_uuid:
        :return:
        """
        pass

    def get_next_node_job_to_execute(self, finished_node_uuid):
        """
        Returns serialized node_job ready to be executed
        :param finished_node_uuid: recently finished node job uuid
        :return:
        If next node job exists and ready to be executed returns serialized node job
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
                    'uuid': next_node_job.uuid.hex,
                    'computing_node_type': next_node_job.computin_node_type,
                    'commands': next_node_job.commands
                }

        return None


