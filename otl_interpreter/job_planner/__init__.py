from otl_interpreter.settings import job_planer_config

from .job_planner import JobPlanner
from .exceptions import JobPlanException


job_planer = JobPlanner(
    job_planer_config['computing_node_type_priority'].split(),
    job_planer_config['subsearch_is_node_job']
)


def plan_job(translated_otl, tws, twf, shared_post_processing=True, subsearch_is_node_job=None):
    return job_planer.plan_job(translated_otl, tws, twf, shared_post_processing, subsearch_is_node_job)




