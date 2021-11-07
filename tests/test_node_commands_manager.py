import uuid

from datetime import datetime
from register_test_commands import register_test_commands
from rest.test import TestCase
from otl_interpreter.interpreter_db import node_commands_manager


class TestGetCommands(TestCase):

    def setUp(self):
        register_test_commands()

    def test_node_types(self):
        types = node_commands_manager.get_node_types()
        self.assertSetEqual(types, {'SPARK', 'EEP', 'POST_PROCESSING'})

    def test_commands_were_updated(self):
        dt = datetime(1, 1, 1)
        self.assertEqual(node_commands_manager.commands_were_updated(dt), True)
        dt = datetime.now()
        self.assertEqual(node_commands_manager.commands_were_updated(dt), False)
        node_guid = uuid.uuid4().hex
        node_commands_manager.register_node('some', node_guid)
        node_commands_manager.register_node_commands(
           node_guid,
           {
               "new_command": {"rules": [
                        {"type": "subsearch"},
                    ]},
                }
        )
        # commands were updated, expect True
        self.assertEqual(node_commands_manager.commands_were_updated(dt), True)














