import logging
import json

from uuid import UUID
from asgiref.sync import sync_to_async


from message_broker import Producer

from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus
from otl_interpreter.interpreter_db import node_job_manager, otl_job_manager

from computing_node_pool import computing_node_pool

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
        NodeJobStatus.SENT_TO_COMPUTING_NODE, NodeJobStatus.IN_QUEUE, NodeJobStatus.CANCELED
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

    def _on_planned(self, node_job_uuid, node_job_dict):
        pass

    def _on_ready_to_execute(self, node_job_uuid, node_job_dict=None):

        # TODO if node job dict is none get node job
        # find computing node to execute
        computing_node_uuid = computing_node_pool.get_least_loaded_node(node_job_dict['computing_node_type'])

        # if not found move node job to queue
        # TODO

        # send node job to computing node
        node_job_dict['uuid'] = node_job_dict['uuid'].hex
        node_job_dict['status'] = NodeJobStatus.READY_TO_EXECUTE

        self._send_message_to_computing_node(computing_node_uuid, json.dumps(node_job_dict))

        self._change_node_job_status(
            node_job_dict['uuid'],
            NodeJobStatus.SENT_TO_COMPUTING_NODE,
            f'Sent to computing node {computing_node_uuid}'
        )

    def _on_in_queue(self, node_job_uuid, node_job_dict):
        pass

    def _on_taken_from_queue(self, node_job_uuid, node_job_dict):
        pass

    def _on_sent_to_computing_node(self, node_job_uuid, node_job_dict):
        pass

    def _on_declined_by_computing_node(self, node_job_uuid, node_job_dict):
        pass

    def _on_running(self, node_job_uuid, node_job_dict):
        pass

    def _on_finished(self, node_job_uuid, node_job_dict):
        log.debug(f'node_job_finished {node_job_uuid}')
        # TODO check job queue fot that node type
        next_node_job_dict = node_job_manager.get_next_node_job_to_execute(node_job_uuid)

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

    def _on_canceled(self, node_job_uuid, node_job_dict):
        node_job_status = node_job_manager.get_node_job_status(node_job_uuid)
        if node_job_status == NodeJobStatus.RUNNING:
            self._cancel_running_node_job(node_job_uuid)

    def _on_failed(self, node_job_uuid, node_job_dict):

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

    def _change_node_job_status(
            self, node_job_uuid: UUID,
            status: NodeJobStatus,
            status_text: str = None,
            node_job_dict: dict = None

    ):
        log.debug(f'Change node job {str(node_job_uuid)} status to {str(status)}. {status_text}')
        node_job_dict = node_job_dict or {}
        cur_status = node_job_manager.get_node_job_status(node_job_uuid)

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




