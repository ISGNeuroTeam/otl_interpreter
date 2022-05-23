import json
import time

from logging import getLogger
from uuid import UUID

from message_broker import Producer

from otl_interpreter.translator import translate_otl
from otl_interpreter.job_planner import plan_job, JobPlanException
from otl_interpreter.settings import ini_config
from otl_interpreter.job_planner import plan_job

from otl_interpreter.interpreter_db import (
    otl_job_manager as db_otl_job_manager, node_job_manager as db_node_job_manager
)

from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus, ResultStatus

log = getLogger('otl_interpreter')


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


class QueryError(Exception):
    pass


class OtlJobManager:
    def __init__(self, default_cache_ttl, default_timeout, default_shared_post_processing, data_path, schema_path):
        self.default_cache_ttl = default_cache_ttl
        self.default_timeout = default_timeout
        self.default_shared_post_processing = default_shared_post_processing
        self.data_path = data_path
        self.schema_path = schema_path

    def makejob(
            self, otl_query, user_guid, tws, twf, otl_job_cache_ttl=None,
            timeout=None, shared_post_processing=None, subsearch_is_node_job=None,
    ):
        """
        :param otl_query: otl query
        :param user_guid: user guid
        :param tws: time window start
        :param twf: time window finish
        :param otl_job_cache_ttl: number of seconds to keep node job results
        :param timeout: timeout for otl job
        :param shared_post_processing: result will be in shared storage or local
        :param subsearch_is_node_job: create new node job for each subsearch or not
        """
        node_job_cache_ttl = otl_job_cache_ttl
        otl_job_cache_ttl = max(otl_job_cache_ttl or self.default_cache_ttl, 30)
        # TODO create check of timeouts
        timeout = timeout or self.default_timeout

        if shared_post_processing is None:
            shared_post_processing = self.default_shared_post_processing

        otl_job_uuid = db_otl_job_manager.make_otl_job(otl_query, user_guid, tws, twf, otl_job_cache_ttl, timeout)

        try:
            translated_query = translate_otl(otl_query)

        except SyntaxError as err:
            log.error(f'Query: {otl_query}\n \nUser_guid: {user_guid} Syntax error:\n {str(err)}')
            db_otl_job_manager.change_otl_job_status(otl_job_uuid, JobStatus.FAILED, f'Syntax error:\n {str(err)}')
            raise QueryError(f'Translation error:\n {err.args[0]}') from err

        db_otl_job_manager.change_otl_job_status(
            otl_job_uuid, JobStatus.TRANSLATED,
            status_text=f'Otl job was successfully translated'
        )
        log.debug(f'otl_query: {otl_query} translated successfully')
        try:
            top_node_job_tree = plan_job(translated_query, tws, twf, shared_post_processing, subsearch_is_node_job)
        except JobPlanException as err:
            log.info(f'Query: {otl_query}\n Job planer error: {str(err)}')
            db_otl_job_manager.change_otl_job_status(otl_job_uuid, JobStatus.FAILED, f'Job planer error: {str(err)}')
            raise QueryError(err.args[0]) from err

        db_node_job_manager.create_node_jobs(top_node_job_tree, otl_job_uuid, otl_job_cache_ttl, node_job_cache_ttl)

        db_otl_job_manager.change_otl_job_status(
            otl_job_uuid, JobStatus.PLANNED,
            status_text=f'Otl job was decomposed on node jobs and planned'
        )

        self._send_new_job_to_dispatcher(top_node_job_tree)

        return otl_job_uuid, top_node_job_tree.result_address.storage_type, top_node_job_tree.result_address.path

    @staticmethod
    def _send_new_job_to_dispatcher(top_node_job_tree):
        """
        sends message to dispatcher with list of NodeJobs to execute in first order
        """
        # find node jobs that don't expect other's results (job tree leaves)
        independent_node_job_trees = top_node_job_tree.leaf_iterator()

        # form message for message broker
        message = json.dumps({
            'command_name': 'NEW_OTL_JOB',
            'command': {
                'node_jobs': [
                    {
                        'uuid': node_job_tree.uuid.hex,
                        'status': NodeJobStatus.PLANNED,
                        'computing_node_type': node_job_tree.computing_node_type,
                        'commands': node_job_tree.as_command_dict_list(),
                        'storage': node_job_tree.result_address.storage_type,
                        'path': node_job_tree.result_address.path,
                    }
                    for node_job_tree in independent_node_job_trees
                ]
            }
        })
        with Producer() as producer:
            message_id = producer.send('otl_job', message)

        return message_id

    @staticmethod
    def check_job(job_id: UUID):
        return db_otl_job_manager.check_job(job_id)

    @staticmethod
    def cancel_job(job_id: UUID, status_text=None):
        db_otl_job_manager.cancel_job(job_id, status_text)
        # send message to dispatcher to cancel node jobs
        unfinished_node_jobs = db_node_job_manager.get_unfinished_node_jobs_for_otl_job(job_id)
        message = json.dumps(
            {
                'command_name': 'CANCEL_JOB',
                'command': {
                    'node_jobs': unfinished_node_jobs
                }
            },  cls=UUIDEncoder
        )
        with Producer() as producer:
            message_id = producer.send('otl_job', message)

    def get_result(self, job_id: UUID):
        result = db_otl_job_manager.get_result(job_id)
        if not result.status == ResultStatus.CALCULATED:
            raise QueryError("Result is not ready yet")

        data_path = f"{result.storage}/{result.path}/jsonl/{self.data_path}"
        schema_path = f"{result.storage}/{result.path}/jsonl/{self.schema_path}"

        return data_path, schema_path


otl_job_manager = OtlJobManager(
    int(ini_config['otl_job_defaults']['cache_ttl']),
    int(ini_config['otl_job_defaults']['timeout']),
    bool(ini_config['otl_job_defaults']['shared_post_processing']),
    ini_config['result_managing']['data_path'],
    ini_config['result_managing']['schema_path']
)

