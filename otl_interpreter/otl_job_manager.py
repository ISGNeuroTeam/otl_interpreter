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

from otl_interpreter.interpreter_db.enums import NodeJobStatus

log = getLogger('otl_interpreter')


class QueryError(Exception):
    pass


class OtlJobManager:
    def __init__(self, default_cache_ttl, default_timeout, default_shared_post_processing):
        self.default_cache_ttl = default_cache_ttl
        self.default_timeout = default_timeout
        self.default_shared_post_processing = default_shared_post_processing

    def makejob(
            self, otl_query, user_guid, tws, twf, cache_ttl=None,
            timeout=None, shared_post_processing=None, subsearch_is_node_job=None,
    ):
        """
        :param otl_query: otl query
        :param user_guid: user guid
        :param tws: time window start
        :param twf: time window finish
        :param cache_ttl: number of seconds to keep node job results
        :param timeout: timeout for otl job
        :param shared_post_processing: result will be in shared storage or local
        :param subsearch_is_node_job: create new node job for each subsearch or not
        """
        cache_ttl = cache_ttl or self.default_cache_ttl
        # TODO create check of timeouts
        timeout = timeout or self.default_timeout

        if shared_post_processing is None:
            shared_post_processing = self.default_shared_post_processing

        try:
            translated_query = translate_otl(otl_query)

        except SyntaxError as err:
            log.error(f'Query: {otl_query}\n \nUser_guid: {user_guid} Syntax error: {str(err)}')
            raise QueryError(f'Tranlation error: {err.args[0]}') from err

        log.debug(f'otl_query: {otl_query} translated successfully')
        try:
            top_node_job_tree = plan_job(translated_query, tws, twf, shared_post_processing, subsearch_is_node_job)
        except JobPlanException as err:
            log.info(f'Query: {otl_query}\n Job planer error: {str(err)}')
            raise QueryError(err.args[0]) from err

        otl_job_uuid = db_otl_job_manager.make_otl_job(otl_query, user_guid, tws, twf, cache_ttl)
        db_node_job_manager.create_node_jobs(top_node_job_tree, otl_job_uuid, cache_ttl)

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
                        'commands': node_job_tree.as_command_dict_list()
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
    def cancel_job(job_id: UUID):
        # send message to dispatcher to cancel node jobs
        message = json.dumps(
            {
                'command_name': 'CANCEL_JOB',
                'command': {
                    'uuid': job_id.hex
                }
            }
        )
        with Producer() as producer:
            message_id = producer.send('otl_job', message)


otl_job_manager = OtlJobManager(
    int(ini_config['otl_job_defaults']['cache_ttl']),
    int(ini_config['otl_job_defaults']['timeout']),
    bool(ini_config['otl_job_defaults']['shared_post_processing'])
)

