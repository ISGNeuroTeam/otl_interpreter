import uuid
from rest.test import TestCase
from otl_interpreter.interpreter_db.node_commands_manager import node_commands_manager


# class TestGetCommands(TestCase):
#     def setUp(self):
#         spark_node_id1 = uuid.uuid4().hex
#         eep_node_id1 = uuid.uuid4().hex
#         post_processing_node_id1 = uuid.uuid4().hex
#
#         node_commands_manager.register_command(
#             'spark', spark_node_id1, 'otstats',
#             [
#                 {
#                     "type": "kwarg",
#                     "key": "index",
#                     "required": True,
#                 },
#             ]
#         )
#         node_commands_manager.register_command(
#             'spark', spark_node_id1, 'collect',
#             [
#                 {
#                     "type": "kwarg",
#                     "key": "index",
#                     "required": True,
#                 },
#             ]
#         )
#         node_commands_manager.register_command(
#             'eep', eep_node_id1, 'sum',
#             [
#                 {
#                     "type": "arg",
#                     "inf": True,
#                 },
#             ]
#         )
#         node_commands_manager.register_command(
#             'post_processing', post_processing_node_id1, 'pp_command1',
#             [
#                 {
#                     "type": "arg",
#                 },
#             ]
#         )
#
#         node_commands_manager.register_command(
#             'all', None, 'pp_command1',
#             [
#                 {
#                     "type": "arg",
#                 },
#             ]
#         )
#
#
#     def test_node_types(self):
#         types = node_commands_manager.get_node_types()
#
#
#
#
#
#
#
