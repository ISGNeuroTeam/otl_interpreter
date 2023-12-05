import random
import logging

from uuid import UUID
from datetime import datetime, timedelta
from collections import defaultdict
from otl_interpreter.settings import ini_config

log = logging.getLogger('otl_interpreter.dispatcher.computing_node_pool')


class ComputingNode:
    def __init__(self, uuid: UUID, node_type, total_resources, local):
        """
        :param uuid: node uuid
        :param node_type: node type
        :param total_resources: dictionary, node resources
        :param local: True if computing node runs on the same host
        :return:
        """
        self.uuid: UUID = uuid
        self.type = node_type
        self.local = local
        self.total_resources = total_resources
        self.used_resources = {key: 0 for key in total_resources.keys()}
        self.last_modified = datetime.now()

    def __gt__(self, other):
        for resource in self.used_resources.keys():
            if self.used_resources[resource] > other.used_resources.get(resource, 0):
                return True
            elif self.used_resources[resource] < other.used_resources.get(resource, 0):
                return False
        return False

    def __eq__(self, other):
        for resource in self.used_resources.keys():
            if self.used_resources[resource] != other.used_resources.get(resource, 0):
                return False
        return True

    def __lt__(self, other):
        for resource in self.used_resources.keys():
            if self.used_resources[resource] < other.used_resources.get(resource, 0):
                return True
            elif self.used_resources[resource] > other.used_resources.get(resource, 0):
                return False
        return False

    def update_used_resources(self, resources):
        self.used_resources.update(resources)
        self.last_modified = datetime.now()

    def all_resources_available(self):
        """
        Return True if computing node have at least one resources of each type.
        False otherwise
        """
        for resource, total_resource_value in self.total_resources.items():
            if self.used_resources[resource] >= self.total_resources[resource]:
                return False
        return True


class ComputingNodePool:
    def __init__(self, health_check_period=10):
        """
        :param health_check_period: health check period in seconds
        If computing node pool didn't receive resource information in that interval computing node considering inactive
        """
        self.nodes_by_types = defaultdict(dict)
        self.nodes = {}

        self.health_check_period: timedelta = timedelta(seconds=health_check_period)

    def __contains__(self, uuid: UUID):
        """
        Checks if uuid in self.nodes
        """
        return uuid in self.nodes

    def add_computing_node(self, uuid: UUID, node_type: str, resources, local):
        """
        :param uuid: node uuid
        :param node_type: node type
        :param resources: dictionary, node resources
        :param local: True if node runs on local host
        :return:
        """
        computing_node = ComputingNode(
            uuid, node_type, resources, local
        )
        self.nodes_by_types[node_type][uuid] = computing_node
        self.nodes[uuid] = computing_node

    def del_computing_node(self, uuid: UUID):
        """
        ;:param uuid: node uuid
        :return:
        """
        if uuid in self.nodes:
            node_type = self.nodes[uuid].type
            del self.nodes_by_types[node_type][uuid]
            del self.nodes[uuid]

    def get_least_loaded_node(self, node_type, only_local_nodes=False):
        """
        Returns uuid of node with lowest resource usage or None
        """
        # exclude nodes with full usage resources
        nodes = tuple(filter(
            lambda node: node.all_resources_available(),
            self.nodes_by_types[node_type].values()
        ))

        # filter locals
        if only_local_nodes:
            nodes = tuple(filter(
                lambda node: node.local,
                nodes
            ))

        if len(nodes) == 1:
            return nodes[0].uuid
        if len(nodes) == 0:
            return None

        min_resources_computing_nodes_uuids = {nodes[0].uuid, }
        min_computing_node = nodes[0]

        for i in range(1, len(nodes)):
            if nodes[i] < min_computing_node:
                min_resources_computing_nodes_uuids.clear()
                min_resources_computing_nodes_uuids.add(nodes[i].uuid)
                min_computing_node = nodes[i]
            elif nodes[i] == min_computing_node:
                min_resources_computing_nodes_uuids.add(nodes[i].uuid)

        if len(min_resources_computing_nodes_uuids) == 1:
            return min_resources_computing_nodes_uuids.pop()
        else:
            # if two equals nodes return random
            return random.choice(tuple(min_resources_computing_nodes_uuids))

    def update_node_resources(self, node_uuid: UUID, resources):
        if node_uuid in self.nodes:
            self.nodes[node_uuid].update_used_resources(
                resources
            )
        else:
            log.error(f'Computing node with uuid={node_uuid} hasn\'t been added in node pool')

    def get_inactive_node_uuids(self):
        """
        Returns uuids list of nodes whose last_modified timestamp was earlier than health_check interval ago
        """
        inactive_node_uuids = list()
        log.debug(str(list(self.nodes.keys())))
        for node_uuid, node in self.nodes.items():
            if node.last_modified < (datetime.now() - self.health_check_period):
                inactive_node_uuids.append(node_uuid)
        return inactive_node_uuids


computing_node_pool = ComputingNodePool(
    int(ini_config['dispatcher']['health_check_period'])
)
