import re
import datetime
import logging
import hashlib

from uuid import UUID
from redis import Redis
from pottery import RedisDict

from core.settings import REDIS_CONNECTION_STRING
from otl_interpreter.otl_job_manager import otl_job_manager, QueryError
from otl_interpreter.interpreter_db.enums import ResultStatus
from otl_interpreter.interpreter_db import otl_job_manager as db_otl_job_manager

log = logging.getLogger('ot_simple_rest_job_proxy')

query_match_reg = re.compile(r'\s*v2\s*\|')

new_status_to_old_status = {
    'NEW': 'new',
    'TRANSLATED': 'running',
    'PLANNED': 'running',
    'RUNNING': 'running',
    'FINISHED': 'success',
    'CANCELED': 'canceled',
    'FAILED': 'failed',
}


class JobProxyManager:
    def __init__(self):
        redis = Redis.from_url(REDIS_CONNECTION_STRING)

        # dictionary where key - hash of otl and tws, twf (otl_query_dict_key), value - query dictionary
        self.new_platform_queries = RedisDict(redis=redis, key='job_proxy_manager_queries')

        # mapping jobid hex string to otl_query_dict_key
        self.new_platform_queries_job_id = RedisDict(redis=redis, key='job_proxy_manager_queries_id')

    def makejob(self, otl_query, user_guid: UUID, tws: str, twf: str, cache_ttl: str):
        """
        Use otl job manager to make a query
        Returns dictionary like makejob endpoint of ot_simple_rest
        """
        otl_query_dict_key = self._make_key_for_query(otl_query, tws, twf)
        # convert time to datetime
        tws = datetime.datetime.fromtimestamp(int(tws))
        twf = datetime.datetime.fromtimestamp(int(twf))

        cache_ttl = int(cache_ttl)
        if otl_query_dict_key in self.new_platform_queries and\
                self.new_platform_queries[otl_query_dict_key]['status'] != 'failed':
            query = self.new_platform_queries[otl_query_dict_key]
            # get result object from database
            end_node_job_result = db_otl_job_manager.get_result(UUID(query['job_id']))

            # if result already exists and calculating or already calculated return success status
            # because check query will be return result of previous task
            if end_node_job_result.status in (ResultStatus.CALCULATED, ResultStatus.CALCULATED):
                return {'status': 'success', 'timestamp': datetime.datetime.now().isoformat()}

        # if result for this otl job not exists make job for it
        try:
            job_id, storage_type, path = otl_job_manager.makejob(otl_query, user_guid, tws, twf, cache_ttl)

            self.new_platform_queries_job_id[job_id.hex] = otl_query_dict_key

            self.new_platform_queries[otl_query_dict_key] = {
                'job_id': job_id.hex,
                'storage_type': storage_type,
                'path': path,
                'status': 'new',
                'status_text': 'Job created'
            }
        except QueryError as err:
            self.new_platform_queries[otl_query_dict_key] = {
                'status': 'failed',
                'status_text': str(err)
            }
            return {'status': 'fail', 'error': str(err)}

        return {'status': 'success', 'timestamp': datetime.datetime.now().isoformat()}

    def checkjob(self, otl_query, tws, twf):
        """
        Use otl job manager to check job status
        Return dectionary like checkjob endpoint of ot_simple_rest
        """
        otl_query_dict_key = self._make_key_for_query(otl_query, tws, twf)

        if otl_query_dict_key not in self.new_platform_queries:
            return {"status": "notfound", "error": "Job is not found"}
        query = self.new_platform_queries[otl_query_dict_key]

        if query['status'] == 'failed':
            return {'status': 'failed', 'error': query['status_text']}

        job_id = UUID(query['job_id'])

        status, status_text = otl_job_manager.check_job(job_id)

        old_status = new_status_to_old_status[status]

        res = {
            'status': old_status
        }

        if old_status in ("failed", "canceled"):
            res['error'] = status_text

        if old_status == 'success':
            res['cid'] = query['job_id']

        return res

    def getresult(self, cid):
        """
        Use otl job manager to get results
        """
        if cid not in self.new_platform_queries_job_id:
            return {'status': 'failed', 'error': f'No cache with id={cid} '}

        query_key = self.new_platform_queries_job_id[cid]
        query = self.new_platform_queries[query_key]
        job_id = UUID(query['job_id'])

        data_path, schema_path = otl_job_manager.get_result(job_id)

        return {
            "status": "success",
            "data_urls": [
                data_path,
                schema_path
            ]
        }

    def is_new_platform_query(selfj, query):
        """
        Analise query content
        Returns index of query begining ( after 'v2 |' string) if query should be sent to new platform, 0 otherwise

        """
        m = query_match_reg.match(query)
        if m:
            return m.end()
        return 0

    def is_new_platform_query_id(self, query_id):
        """
        Returns true id query with id was sent to new platform
        """
        return query_id in self.new_platform_queries_job_id

    def _make_key_for_query(self, query, tws, twf):
        """
        Makes unique key for query
        """
        h = hashlib.md5((query + str(tws) + str(twf)).encode())
        return h.digest().hex()


job_proxy_manager = JobProxyManager()
