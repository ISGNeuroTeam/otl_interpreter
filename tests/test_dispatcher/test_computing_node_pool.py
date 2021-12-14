from unittest import TestCase

from otl_interpreter.dispatcher.computing_node_pool import ComputingNodePool, ComputingNode
from otl_interpreter.interpreter_db.enums import ComputingNodeType


class TestComputingNodePool(TestCase):
    def test_add_node(self):
        computing_node_pool = ComputingNodePool()
        testUUID = 'testUUID'
        computing_node_pool.add_computing_node(
            testUUID, ComputingNodeType.SPARK.value,
            {
                'job_capacity': 10
            }
        )
        computing_node = computing_node_pool.nodes_by_types[ComputingNodeType.SPARK.value][testUUID]
        self.assertEqual(
            type(computing_node),
            ComputingNode
        )
        self.assertEqual(
            computing_node.uuid,
            testUUID
        )

    def test_zero_compuging_nodes(self):
        computing_node_pool = ComputingNodePool()
        node = computing_node_pool.get_least_loaded_node('SPARK')
        self.assertIsNone(node)

    def test_one_computing_node(self):
        computing_node_pool = ComputingNodePool()
        computing_node_pool.add_computing_node(
            'test1',
            'SPARK',
            {
                'test_resource': 20
            }
        )
        node = computing_node_pool.get_least_loaded_node('SPARK')
        self.assertEqual(node.uuid, 'test1')

    def test_computing_node_comparison_one_resourse(self):
        node1 = ComputingNode(
            'test1',
            'SPARK',
            {
                'test_resource': 20
            }
        )

        node2 = ComputingNode(
            'test2',
            'SPARK',
            {
                'test_resource': 20
            }
        )

        node1.update_used_resources({
            'test_resource': 10
        })

        node2.update_used_resources({
            'test_resource': 20
        })

        self.assertTrue(node2 > node1)
        self.assertTrue(node1 < node2)

    def test_computing_node_comparison_many_resourse(self):
        node1 = ComputingNode(
            'test1',
            'SPARK',
            {
                'test_resource': 100,
                'test_resource2': 100,
                'test_resource3': 100
            }
        )

        node1.update_used_resources(
            {
                'test_resource': 10,
                'test_resource2': 10,
                'test_resource3': 10
            }
        )

        node2 = ComputingNode(
            'test2',
            'SPARK',
            {
                'test_resource': 200,
                'test_resource2': 200,
                'test_resource3': 200
            }
        )

        node2.update_used_resources(
            {
                'test_resource': 10,
                'test_resource2': 10,
                'test_resource3': 20
            }
        )

        self.assertTrue(node2 > node1)
        self.assertTrue(node1 < node2)

        min_node = min(node1, node2)
        self.assertEqual(
            min_node.uuid,
            'test1'
        )

    def test_get_least_loaded_node_with_one_resource(self):
        computing_node_pool = ComputingNodePool()
        computing_node_pool.add_computing_node(
            'test1',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test1',
            {
                'first_resource': 9
            }
        )
        computing_node_pool.add_computing_node(
            'test2',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test2',
            {
                'first_resource': 7
            }
        )

        least_loaded_node_uuid = computing_node_pool.get_least_loaded_node('SPARK')
        self.assertEqual(
            least_loaded_node_uuid,
            'test2'
        )

    def test_get_least_loaded_node_with_two_resource(self):
        computing_node_pool = ComputingNodePool()

        computing_node_pool.add_computing_node(
            'test0',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test0',
            {
                'first_resource': 15
            }
        )
        computing_node_pool.add_computing_node(
            'test-1',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test-1',
            {
                'first_resource': 15
            }
        )

        computing_node_pool.add_computing_node(
            'test1',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test1',
            {
                'first_resource': 9,
                'second_resource': 9
            }
        )

        computing_node_pool.add_computing_node(
            'test2',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test2',
            {
                'first_resource': 9,
                'second_resource': 7
            }
        )

        computing_node_pool.add_computing_node(
            'test3',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )

        computing_node_pool.update_node_resources(
            'test3',
            {
                'first_resource': 10,
                'second_resource': 10
            }
        )

        least_loaded_node_uuid = computing_node_pool.get_least_loaded_node('SPARK')
        self.assertEqual(
            least_loaded_node_uuid,
            'test2'
        )

    def test_get_least_loaded_equal_nodes(self):
        computing_node_pool = ComputingNodePool()
        computing_node_pool.add_computing_node(
            'test1',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test1',
            {
                'first_resource': 10,
                'second_resource': 10
            }
        )

        computing_node_pool.add_computing_node(
            'test2',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test2',
            {
                'first_resource': 15,
                'second_resource': 15
            }
        )

        computing_node_pool.add_computing_node(
            'test3',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test3',
            {
                'first_resource': 10,
                'second_resource': 10
            }
        )
        computing_node_pool.add_computing_node(
            'test4',
            ComputingNodeType.SPARK.value,
            {
                'first_resource': 100,
                'second_resource': 100
            }
        )
        computing_node_pool.update_node_resources(
            'test4',
            {
                'first_resource': 15,
                'second_resource': 15
            }
        )

        # must return test1 or test3
        node_uuids = set()
        for i in range(20):
            node_uuid = computing_node_pool.get_least_loaded_node('SPARK')
            node_uuids.add(node_uuid)
            self.assertIn(node_uuid, ('test1', 'test3'))

        self.assertIn('test1', node_uuids)
        self.assertIn('test3', node_uuids)



