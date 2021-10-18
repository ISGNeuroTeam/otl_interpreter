from otl_interpreter.settings import job_planer_config

from .job_planner import JobPlanner
from .exceptions import JobPlanException


job_planer = JobPlanner(job_planer_config['computing_node_type_priority'])


def plan_job(translated_otl):
    job_planer.plan_job(translated_otl)



