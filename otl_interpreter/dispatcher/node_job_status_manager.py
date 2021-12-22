import logging
import json
import datetime

from uuid import UUID
from asgiref.sync import sync_to_async


from message_broker import Producer

from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus, ComputingNodeType
from otl_interpreter.interpreter_db import node_job_manager, otl_job_manager

from computing_node_pool import computing_node_pool

from node_job_queue import node_job_queue


log = logging.getLogger('otl_interpreter.dispatcher')


# sets of allowed next states
allowed_state_transfer_table = {
    NodeJobStatus.PLANNED: {
        NodeJobStatus.READY_TO_EXECUTE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.READY_TO_EXECUTE: {
        NodeJobStatus.IN_QUEUE, NodeJobStatus.SENT_TO_COMPUTING_NODE, NodeJobStatus.FINISHED, NodeJobStatus.CANCELED
    },
    NodeJobStatus.IN_QUEUE: {
        NodeJobStatus.TAKEN_FROM_QUEUE, NodeJobStatus.CANCELED
    },
    NodeJobStatus.TAKEN_FROM_QUEUE: {
        NodeJobStatus.READY_TO_EXECUTE,
    },
    NodeJobStatus.SENT_TO_COMPUTING_NODE: {
        NodeJobStatus.RUNNING, NodeJobStatus.FAILED, NodeJobStatus.FINISHED, NodeJobStatus.DECLINED_BY_COMPUTING_NODE, NodeJobStatus.CANCELED
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


class NodeJobStatusManager:
    """
    Manages node job status
    """
    def __init__(self):
        self.status_action_table = {
            NodeJobStatus.PLANNED: self._on_planned,
            NodeJobStatus.READY_TO_EXECUTE: self._on_ready_to_execute,
            NodeJobStatus.IN_QUEUE: self._on_in_queue,
            NodeJobStatus.TAKEN_FROM_QUEUE: self._on_taken_from_queue,
            NodeJobStatus.SENT_TO_COMPUTING_NODE: self._on_sent_to_computing_node,
            NodeJobStatus.DECLINED_BY_COMPUTING_NODE: self._on_declined_by_computing_node,
            NodeJobStatus.RUNNING: self._on_running,
            NodeJobStatus.FINISHED: self._on_finished,
            NodeJobStatus.CANCELED: self._on_canceled,
            NodeJobStatus.FAILED: self._on_failed,
        }

        # if Producer() throws error del method shouldn't invoke producer.stop()
        self.producer = None

        self.producer = Producer()
        self.producer.start()

    def __del__(self):
        if self.producer:
            self.producer.stop()

    @staticmethod
    def is_next_node_job_status_allowed(cur_status, next_status):
        return next_status in allowed_state_transfer_table[cur_status]

    @sync_to_async
    def change_node_job_status(
            self, node_job_uuid: UUID,
            status: NodeJobStatus,
            status_text: str,
            node_job_dict: dict = None

    ):
        """
        Changes node job status if changing is allowed by status transfer table
        Makes necessary actions on status change
        :param node_job_uuid:
        :param status:
        :param status_text:
        :param node_job_dict: dictinary representing node job ( for optimization ), node_job_dict
        :return:
        """
        return self._change_node_job_status(node_job_uuid, status, status_text, node_job_dict)

    @sync_to_async
    def check_job_queue(self):
        """
        Task to check periodicaly node job queue
        """
        for computing_node_type in ComputingNodeType:
            self._check_job_queue(computing_node_type)

    # ==================================================================================================================
    # _on_* functions are "callbacks" on node job state changing

    def _on_planned(self, node_job_uuid, node_job_dict=None):
        pass

    def _on_ready_to_execute(self, node_job_uuid, node_job_dict=None):

        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        # find computing node to execute
        computing_node_uuid = computing_node_pool.get_least_loaded_node(node_job_dict['computing_node_type'])

        # if not found move node job to queue
        if computing_node_uuid is None:
            node_job_queue.add(
                node_job_dict,
                self._calculate_node_job_priority_for_queue()
            )
            self._change_node_job_status(
                node_job_uuid, NodeJobStatus.IN_QUEUE,
                f'Moved to queue because available computing node not found',
                node_job_dict
            )
            return

        # send node job to computing node
        node_job_dict['uuid'] = node_job_dict['uuid'].hex
        node_job_dict['status'] = NodeJobStatus.READY_TO_EXECUTE

        self._send_message_to_computing_node(computing_node_uuid, json.dumps(node_job_dict))

        self._change_node_job_status(
            node_job_dict['uuid'],
            NodeJobStatus.SENT_TO_COMPUTING_NODE,
            f'Sent to computing node {computing_node_uuid}'
        )

    def _on_in_queue(self, node_job_uuid, node_job_dict=None):
        pass

    def _on_taken_from_queue(self, node_job_uuid, node_job_dict=None):
        # todo check computing node availability without change node job state???
        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        self._change_node_job_status(
            node_job_uuid,
            NodeJobStatus.READY_TO_EXECUTE,
            f'Checking for avalable computing node',
            node_job_dict
        )

    def _on_sent_to_computing_node(self, node_job_uuid, node_job_dict=None):
        pass

    def _on_declined_by_computing_node(self, node_job_uuid, node_job_dict=None):
        # just put job to the queue
        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        self._put_node_job_in_queue(node_job_dict)

        self._change_node_job_status(
            node_job_uuid, NodeJobStatus.IN_QUEUE,
            f'Moved to queue because was declined by computing node',
            node_job_dict
        )

    def _on_running(self, node_job_uuid, node_job_dict=None):
        pass

    def _on_finished(self, node_job_uuid, node_job_dict=None):
        log.debug(f'node_job_finished {node_job_uuid}')

        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        next_node_job_dict = node_job_manager.get_next_node_job_to_execute(node_job_uuid)

        # if next node job exists launch it else set otl job finished
        if next_node_job_dict:
            self._change_node_job_status(
                next_node_job_dict['uuid'], NodeJobStatus.READY_TO_EXECUTE,
                f'Children node jobs are finished',
                next_node_job_dict
            )
        else:
            otl_job_uuid = node_job_manager.get_otl_job_uuid(node_job_uuid)
            otl_job_manager.change_otl_job_status(
                otl_job_uuid, JobStatus.FINISHED,
                f'All node jobs successfully finished'
            )

        # check job queue for that computing node type
        self._check_job_queue(node_job_dict['computing_node_type'])

    def _on_canceled(self, node_job_uuid, node_job_dict=None):
        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        if node_job_dict['status'] == NodeJobStatus.RUNNING:
            self._cancel_running_node_job(node_job_uuid)

            # also check job queue
            self._check_job_queue(node_job_dict['computing_node_type'])

    def _on_failed(self, node_job_uuid, node_job_dict=None):
        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        otl_job_uuid = node_job_manager.get_otl_job_uuid(node_job_uuid)
        otl_job_manager.change_otl_job_status(
            otl_job_uuid,
            JobStatus.FAILED,
            f'Failed because of node job: {str(node_job_manager.get_node_job_dict(node_job_uuid))}'
        )
        # find other running jobs and cancel them
        running_jobs_uuids = node_job_manager.get_node_jobs_for_cancelling(node_job_uuid)

        log.debug(f'Running jobs for canceling: {str(running_jobs_uuids)}')

        for job_uuid in running_jobs_uuids:
            self._change_node_job_status(
                job_uuid, NodeJobStatus.CANCELED,
                f'Cancelled because of node job fail: {str(node_job_manager.get_node_job_dict(node_job_uuid))}'
            )

        self._check_job_queue(node_job_dict['computing_node_type'])

    # ==================================================================================================================

    def _change_node_job_status(
            self, node_job_uuid: UUID,
            status: NodeJobStatus,
            status_text: str = None,
            node_job_dict: dict = None

    ):
        log.debug(f'Change node job {str(node_job_uuid)} status to {str(status)}. {status_text}')

        if node_job_dict is None:
            node_job_dict = node_job_manager.get_node_job_dict(node_job_uuid)

        cur_status = node_job_dict['status']

        # if status the same do nothing
        if cur_status == status and status_text is not None:
            # just refresh status text
            log.debug(f'Refresh node job status, node job uuid {node_job_uuid}, status: {status_text}')
            node_job_manager.change_node_job_status(node_job_uuid, status, status_text)
            return

        if not self.is_next_node_job_status_allowed(cur_status, status):
            log.warning(
                f'Trying set not allowed node job status. NodeJob uuid: {node_job_uuid},\
                 status: {cur_status}, next status: {status}'
            )
            return

        # set status on db
        node_job_manager.change_node_job_status(node_job_uuid, status, status_text)
        # change status in node_job_dict
        node_job_dict['status'] = status

        # make actions on state change
        self.status_action_table[status](node_job_uuid, node_job_dict)

    def _cancel_running_node_job(self, node_job_uuid):
        computing_node_uuid = node_job_manager.get_computing_node_for_node_job(node_job_uuid)
        message_dict = {
            'uuid': node_job_uuid.hex,
            'status': NodeJobStatus.CANCELED
        }
        self._send_message_to_computing_node(computing_node_uuid, json.dumps(message_dict))

    def _send_message_to_computing_node(self, computing_node_uuid, message):
        """
        Sends message to topic for computing node
        :param computing_node_uuid: compurtingg node uuid
        :param message: message, str or bytes
        :return:
        """
        self.producer.send(f"{computing_node_uuid.hex}_job", message)

    def _check_job_queue(self, computing_node_type):
        """
        Checks len of job queue for computing node type and take one node job
        When node job is taken the next state method will try to send it to computing node
        """
        if node_job_queue.node_jobs_in_queue(computing_node_type):
            node_job, priority = node_job_queue.pop(computing_node_type)
            self._change_node_job_status(
                node_job['uuid'],
                NodeJobStatus.TAKEN_FROM_QUEUE,
                f'Taken from queue to find computing node',
                node_job
            )

    def _put_node_job_in_queue(self, node_job_dict):
        # set status IN_QUEUE to node job dict
        # so when it is taken from queue the status is correct
        node_job_queue_dict = node_job_dict.copy()
        node_job_queue_dict['status'] = NodeJobStatus.IN_QUEUE

        node_job_queue.add(
            node_job_queue_dict,
            self._calculate_node_job_priority_for_queue(),
        )

    @staticmethod
    def _calculate_node_job_priority_for_queue():
        return datetime.datetime.now().timestamp()




