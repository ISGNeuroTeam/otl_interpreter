from datetime import timedelta
from otl_interpreter.interpreter_db.models import NodeJob, NodeJobResult, OtlJob


class NodeJobManager:

    def __init__(self, default_cache_ttl):
        self.default_cache_ttl = default_cache_ttl

    def create_node_jobs(self, root_job_tree, otl_job_uuid, cache_ttl=None):
        """
        Creates in database NodeJob tree
        :param root_job_tree: job planer NodeJobTree
        :param otl_job_uuid: otl job uuid
        :param cache_ttl: time to store node job results
        :return:
        """
        cache_ttl = cache_ttl or self.default_cache_ttl
        otl_job = OtlJob.objects.get(uuid=otl_job_uuid)
        node_job_for_job_tree = {}

        for node_job_tree in root_job_tree.parent_first_order_traverse_iterator():

            if node_job_tree.parent:
                parent_node_job = node_job_for_job_tree[node_job_tree.parent]
            else:
                parent_node_job = None

            result_address = node_job_tree.result_address
            if result_address:
                node_job_result = NodeJobResult(
                    storage=result_address.storage_type,
                    path=result_address.path,
                    ttl=timedelta(cache_ttl),
                )
                node_job_result.save()
            else:
                node_job_result = None

            node_job = NodeJob(
                otl_job=otl_job,
                uuid=node_job_tree.uuid,
                computing_node_type=node_job_tree.computing_node_type,
                commands=node_job_tree.as_command_dict_list(),
                next_job=parent_node_job,
                result=node_job_result,
            )
            node_job.save()
            node_job_for_job_tree[node_job_tree] = node_job


