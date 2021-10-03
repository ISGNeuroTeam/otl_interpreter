from rest.test import TestCase
from py_otl_parser import Parser
from otl_interpreter.interpreter_db import node_commands_manager
from pprint import pp

from register_test_commands import register_test_commands


class TestSimpleParsing(TestCase):
    def setUp(self):
        register_test_commands()
        self.commands = node_commands_manager.get_commands_syntax()
        self.parser = Parser()

    def test_simple_parse(self):
        otl_query = "| readfile arg1, arg2, arg3"
        expected_result = [{'command': {'value': 'readfile', 'type': 'term'}, 'commandargs': [{'value': {'value': 'arg1', 'type': 'term'}, 'type': 'arg'}, {'value': {'value': 'arg2', 'type': 'term'}, 'type': 'arg'}, {'value': {'value': 'arg3', 'type': 'term'}, 'type': 'arg'}]}]
        parsed_otl = self.parser.parse("| readfile arg1, arg2, arg3", self.commands)

        self.assertEqual(expected_result, parsed_otl)

    def test_parse_with_syntax_error(self):
        with self.assertRaises(SyntaxError):
            self.parser.parse("| readfile2 arg3", self.commands)

    def test_subsearch(self):
        parsed_otl = self.parser.parse("async name=\"s3\", [|readfile 1, 2, 3 ]| table a, b, c, key=val \n| join [| sum a, b | await name='s3', override=True]", self.commands)

    def test_print_subsearches(self):
        parsed_otl = self.parser.parse("| sum arg1, 3, 'asdf', 4.3434 ", self.commands)
        pp(parsed_otl)
        parsed_otl = self.parser.parse("| otstats index=asdf | otstats index='asdsdf' | otstats index=3 | otstats index=4.234", self.commands)
        pp(parsed_otl)




