import datetime
from core.celeryapp import app
from otl_interpreter.otl_job_manager import otl_job_manager


@app.task()
def makejob(otl_query, user_guid, tws=None, twf=None, twds=None, twdf=None, cache_ttl=None,
            timeout=None, shared=None, subsearch_is_node_job=None):
    """
    Task for celery. Invokes otl_job_manager.makejob. It is possible to set time window start delta (twds)
    and time window finish delta (twdf).
    time window start delta (twds) - timedelta from current time ( datetime.now() ) which will be time window start for makejob
    can be only positive integer, 0 - current time, 3600 - two minuter ago
    time window finish delta (twdf) - timedelta from current time which will be time window finish for makejob
    :param otl_query: otl query
    :param user_guid: user guid
    :param tws: time window start
    :param twf: time windw finish
    :param twds: time window start delta
    :param twdf: time window finish delta
    :param cache_ttl: number of seconds to keep node job results
    :param timeout: timeout for otl job
    :param shared: result will be in shared storage or local
    :param subsearch_is_node_job: create new node job for each subsearch or not
    :return:
    """
    if tws is None and twds is None:
        raise ValueError('makejob need tws or twds arguments')
    if twf is None and twds is None:
        raise ValueError('makejob need twf or twfs arguments')

    tws = tws or datetime.datetime.now() - datetime.timedelta(twds)
    twf = twf or datetime.datetime.now() - datetime.timedelta(twdf)

    otl_job_manager.makejob(otl_query, user_guid, tws, twf, cache_ttl, timeout, shared, subsearch_is_node_job)
