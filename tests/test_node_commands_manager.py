from register_test_commands import register_test_commands
from rest.test import TestCase
from otl_interpreter.interpreter_db import node_commands_manager


class TestGetCommands(TestCase):

    def setUp(self):
        register_test_commands()

    def test_node_types(self):
        types = node_commands_manager.get_node_types()
        self.assertSetEqual(types, {'spark', 'eep', 'post_processing'})











