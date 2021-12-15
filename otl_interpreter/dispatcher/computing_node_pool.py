import random

from message_broker import AsyncProducer
from otl_interpreter.interpreter_db.enums import ComputingNodeType


class ComputingNode:
    def __init__(self, uuid, node_type, total_resources):
        """
        :param uuid: node uuid
        :param node_type: node type
        :param total_resources: dictionary, node resources
        :return:
        """
        self.uuid = uuid
        self.type = node_type
        self.total_resources = total_resources
        self.used_resources = {key: 0 for key in total_resources.keys()}

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


class ComputingNodePool:
    def __init__(self):
        self.nodes_by_types = {key: {} for key in ComputingNodeType}
        self.nodes = {}

    def add_computing_node(self, uuid, node_type, resources):
        """
        :param uuid: node uuid
        :param node_type: node type
        :param resources: dictionary, node resources
        :return:
        """
        computing_node = ComputingNode(
            uuid, node_type, resources
        )
        self.nodes_by_types[node_type][uuid] = computing_node
        self.nodes[uuid] = computing_node

    def del_computing_node(self, uuid):
        """
        ;:param uuid: node uuid
        :return:
        """
        node_type = self.nodes[uuid].type
        del self.nodes_by_types[node_type][uuid]
        del self.nodes[uuid]

    def get_least_loaded_node(self, node_type):
        """
        Returns uuid of node with lowest resource usage or None
        """
        nodes = tuple(self.nodes_by_types[node_type].values())
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

    def update_node_resources(self, node_uuid, resources):
        self.nodes[node_uuid].update_used_resources(
            resources
        )


computing_node_pool = ComputingNodePool()
