import re
from redis import Redis
from pottery import RedisDict

from core.settings import REDIS_CONNECTION_STRING
from otl_interpreter.otl_job_manager import otl_job_manager


query_match_reg = re.compile(r'\s*v2\s*\|')


class JobProxyManager:
    def __init__(self):
        redis = Redis.from_url(REDIS_CONNECTION_STRING)
        self.query = RedisDict(redis=redis, key='job_proxy_manager_queries')


    def makejob(self, otl_query, tws, twf, cache_ttl):
        """
        Use otl job manager to make a query
        Returns dictionary like makejob endpoint of ot_simple_rest
        """
        pass

    def checkjob(self):
        """
        Use otl job manager to check job status
        Return dectionary like checkjob endpoint of ot_simple_rest
        """
        pass

    def getresult(self):
        """
        Use otl job manager to get
        """
        pass

    def is_new_platform_query(selfj, query):
        """
        Analise query content
        Returns true if query should be sent to new platform, false otherwise

        """
        return bool(query_match_reg.match(query))

    def is_new_platform_query_id(self, query_id):
        """
        Returns true id query with id was sent to new platform
        """
        return query_id in self.query

    def _make_key_for_query(self, query, tws, twf):
        """
        Makes unique key for query
        """
        return hash(query + str(tws), + str(twf))


job_proxy_manager = JobProxyManager()
