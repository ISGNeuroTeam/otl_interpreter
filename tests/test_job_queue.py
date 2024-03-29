import random
import string
import uuid
import datetime

from core.settings.test import REDIS_CONFIG
from rest.test import TestCase
from otl_interpreter.utils.priority_queue import RedisPriorityQueue
from otl_interpreter.utils.priority_queue import PriorityQueue

from otl_interpreter.dispatcher.node_job_queue import NodeJobQueue

from register_test_commands import register_test_commands


class BaseTestCases:
    class TestPriorityQueue(TestCase):

        def setUp(self) -> None:
            self.queue = None

        def tearDown(self) -> None:
            del self.queue

        def test_push_pop(self):
            self.queue.add(12, b'4')
            self.queue.add(10, '3')
            elements = self.queue.pop()

            self.assertEqual(elements[0][0], b'4')

        def test_push_pop_several(self):
            self.queue.add(12, '4')
            self.queue.add(10, '3')
            self.queue.add(13, '23')

            elements = self.queue.pop(count=2)

            self.assertEqual(len(elements), 2)
            self.assertEqual(elements[1][0], b'4')

        def test_pop_minscore(self):
            self.queue.add(12, '4')
            self.queue.add(10, '3')
            self.queue.add(13, '23')

            elements = self.queue.pop(count=2, min_score=True)
            self.assertEqual(elements[0][0], b'3')
            self.assertEqual(elements[1][0], b'4')

        def test_updated_score(self):
            self.queue.add(3, 'element')
            self.queue.add(4.0, 'element')
            elements = self.queue.pop()
            self.assertEqual(elements[0][1], 4.0)

        def test_lexic_order_with_same_score(self):
            def _check_lexic_order(min_score=False):
                self.queue.add(1, 'abc')
                self.queue.add(1, 'aac234')
                self.queue.add(1, 'acc')
                elements = self.queue.pop(count=3, min_score=min_score)
                self.assertListEqual(
                    [element[0] for element in elements],
                    list(sorted([b'aac234', b'abc', b'acc', ], reverse=not min_score))
                )
            _check_lexic_order()
            _check_lexic_order(True)

        def test_score_groups(self):
            self.queue.add(1, 'abs')
            self.queue.add(2, 'abc')
            self.queue.add(2, 'aaa')
            self.queue.add(2, 'accc')
            self.queue.add(3, 'cbb')

            elements = self.queue.pop(count=3)
            self.assertListEqual(
                [element[0] for element in elements],
                [b'cbb', b'accc', b'abc',]
            )

        def test_pop_from_empty(self):
            elements = self.queue.pop(count=23)
            self.assertListEqual(elements, [])

        def test_pop_too_mush(self):
            self.queue.add(1, 'asdf')
            elements =  self.queue.pop(count=20)
            self.assertEqual(len(elements), 1)
            self.assertTupleEqual(elements[0], (b'asdf', 1.0))

        def test_length(self):
            queue_size = random.randint(30, 50)
            for i in range(queue_size):
                self.queue.add(
                    random.randint(1, 100),
                    ''.join(random.choice(
                        string.ascii_lowercase + string.digits) for _ in range(random.randint(3, 10))
                    )

                )
            pop_count = random.randint(10, 20)

            self.queue.pop(count=pop_count)
            self.assertEqual(queue_size-pop_count, len(self.queue))


class TestRedisPriorityQueue(BaseTestCases.TestPriorityQueue):
    def setUp(self):
        self.queue = RedisPriorityQueue(
            'test_queue',
            REDIS_CONFIG
        )


class TestPriorityQueue(BaseTestCases.TestPriorityQueue):
    def setUp(self) -> None:
        self.queue = PriorityQueue()

def _get_test_computing_node_dict(status=None, computing_node_type=None):
    return {
        'uuid': uuid.uuid4(),
        'status': status or "FINISHED",
        'computing_node_type': computing_node_type or "POST_PROCESSING",
        'commands': [],
        'storage': "shared_post_processing",
        'path': "fff",
        'user_guid': uuid.uuid4()
    }

class BaseNodeJobQueueTestCases:

    class TestNodeJobQueue(TestCase):
        def setUp(self) -> None:
            self.node_job_queue = None

        def test_queue_class_choice(self):
            node_job_queue = NodeJobQueue(one_process_mode=True)
            self.assertEqual(type(node_job_queue.queues['SPARK']), PriorityQueue)
            node_job_queue = NodeJobQueue()
            self.assertEqual(type(node_job_queue.queues['SPARK']), RedisPriorityQueue)

        def test_simple_add_pop(self):
            node_job_dict1 = _get_test_computing_node_dict(computing_node_type='SPARK')
            node_job_dict2 = _get_test_computing_node_dict(computing_node_type='SPARK')

            score = datetime.datetime.now().timestamp()
            self.node_job_queue.add(node_job_dict1, score)
            score2 = datetime.datetime.now().timestamp() + 1
            self.node_job_queue.add(node_job_dict2, score2)

            node_job_dict, priority_score = self.node_job_queue.pop('SPARK')
            self.assertDictEqual(node_job_dict, node_job_dict1)

            node_job_dict, priority_score = self.node_job_queue.pop('SPARK')
            self.assertDictEqual(node_job_dict, node_job_dict2)

        def test_computing_node_type_keys(self):
            node_job_dicts = [
                _get_test_computing_node_dict(
                    computing_node_type=''.join(random.choices(string.ascii_uppercase, k=random.randint(4, 10)))
                )
                for i in range(10)
            ]
            score = datetime.datetime.now().timestamp()
            for node_job_dict in node_job_dicts:
                self.node_job_queue.add(node_job_dict, score)

            node_types = set(
                map(
                    lambda x: x['computing_node_type'], node_job_dicts
                )
            )

            self.assertSetEqual(node_types, set(self.node_job_queue.computing_node_types()))



class TestNodeJobQueueOnRedis(BaseNodeJobQueueTestCases.TestNodeJobQueue):
    def setUp(self) -> None:
        register_test_commands()
        self.node_job_queue = NodeJobQueue(one_process_mode=False)


class TestNodeJobQueueOnOrdinaryPriorityQueue(BaseNodeJobQueueTestCases.TestNodeJobQueue):
    def setUp(self) -> None:
        register_test_commands()
        self.node_job_queue = NodeJobQueue(one_process_mode=True)
