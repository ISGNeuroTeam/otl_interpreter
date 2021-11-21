from .node_commands_manager_class import NodeCommandsManager
from .node_job_manager_class import NodeJobManager
from .otl_job_manager_class import OtlJobManager

from otl_interpreter.settings import ini_config

__all__ = ['node_commands_manager', 'node_job_manager', 'otl_job_manager']

node_commands_manager = NodeCommandsManager()
node_job_manager = NodeJobManager(int(ini_config['node_job']['cache_ttl']))
otl_job_manager = OtlJobManager()
