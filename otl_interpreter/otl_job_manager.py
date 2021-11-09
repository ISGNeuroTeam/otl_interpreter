from logging import getLogger

from otl_interpreter.translator import translate_otl
from otl_interpreter.job_planner import plan_job, JobPlanException
from otl_interpreter.settings import ini_config
from otl_interpreter.job_planner import plan_job

from otl_interpreter.interpreter_db import (
    otl_job_manager as db_otl_job_manager, node_job_manager as db_node_job_manager
)

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
        :param twf: time windw finish
        :param cache_ttl: number of seconds to keep node job results
        :param timeout: timeout for otl job
        :param shared: result will be in shared storage or local
        :param subsearch_is_node_job: create new node job for each subsearch or not
        """
        cache_ttl = cache_ttl or self.default_cache_ttl
        # TODO create check of timeouts
        timeout = timeout or self.default_timeout

        shared_post_processing = shared_post_processing or self.default_shared_post_processing

        try:
            translated_query = translate_otl(otl_query)

        except SyntaxError as err:
            log.error(f'Query: {otl_query}\n \nUser_guid: {user_guid} Syntax error: {str(err)}')
            raise QueryError(err.args[0]) from err

        log.debug(f'otl_query: {otl_query} translated successfully')
        try:
            node_job_tree = plan_job(translated_query, tws, twf, shared_post_processing, subsearch_is_node_job)
        except JobPlanException as err:
            log.info(f'Query: {otl_query}\n Job planer error: {str(err)}')
            raise QueryError(err.args[0]) from err

        otl_job_uuid = db_otl_job_manager.make_otl_job(otl_query, user_guid, tws, twf, cache_ttl)
        db_node_job_manager.create_node_jobs(node_job_tree, otl_job_uuid, cache_ttl)
        # todo signal to dispatcher
        return otl_job_uuid


otl_job_manager = OtlJobManager(
    int(ini_config['otl_job_defaults']['cache_ttl']),
    int(ini_config['otl_job_defaults']['timeout']),
    bool(ini_config['otl_job_defaults']['shared_post_processing'])
)

