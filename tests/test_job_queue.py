from rest.test import TestCase
from otl_interpreter.utils.redis_priority_queue import RedisPriorityQueue


class TestRedisQueue(TestCase):
    def setUp(self):
        self.queue = RedisPriorityQueue(
            'test_queue',
            {
                'host': 'localhost',
                'port': '6379',
                'db': 0,
                'password': '',
            }
        )

    def tearDown(self) -> None:
        del self.queue

    def test_push_pop(self):
        self.queue.push(12, b'4')
        self.queue.push(10, '3')
        elements = self.queue.pop()

        self.assertEqual(elements[0][0], b'4')

    def test_push_pop_several(self):
        self.queue.push(12, '4')
        self.queue.push(10, '3')
        self.queue.push(13, '23')

        elements = self.queue.pop(count=2)

        self.assertEqual(len(elements), 2)
        self.assertEqual(elements[1][0], b'4')

    def test_pop_minscore(self):
        self.queue.push(12, '4')
        self.queue.push(10, '3')
        self.queue.push(13, '23')

        elements = self.queue.pop(count=2, min_score=True)
        self.assertEqual(elements[0][0], b'3')
        self.assertEqual(elements[1][0], b'4')
        print(elements)



