from rest.test import TestCase
from translate_otl import translate_otl
from otl_interpreter.interpreter_db import node_commands_manager
from pprint import pp

from register_test_commands import register_test_commands


class TestSimpleParsing(TestCase):
    def setUp(self):
        register_test_commands()

    def test_simple_parse(self):
        otl_query = "| readfile arg1, arg2, arg3"
        expected_result = [{'command': {'value': 'readfile', 'type': 'term', 'leaf_type': 'simple'}, 'commandargs': [[{'value': {'value': 'arg1', 'type': 'term', 'leaf_type': 'simple'}, 'type': 'arg', 'leaf_type': 'complex'}], [{'value': {'value': 'arg2', 'type': 'term', 'leaf_type': 'simple'}, 'type': 'arg', 'leaf_type': 'complex'}], [{'value': {'value': 'arg3', 'type': 'term', 'leaf_type': 'simple'}, 'type': 'arg', 'leaf_type': 'complex'}]]}]

        parsed_otl = translate_otl(otl_query)
        self.assertEqual(expected_result, parsed_otl)

    def test_parse_with_syntax_error(self):
        with self.assertRaises(SyntaxError):
            translate_otl("| readfile2 arg3")

    def test_subsearch(self):
        parsed_otl = translate_otl("async name=\"s3\", [|readfile 1, 2, 3 ]| table a, b, c, key=val \n| join [| sum a, b | await name='s3', override=True]")

    def test_print_subsearches(self):
        parsed_otl = translate_otl("| sum arg1, 3, 'asdf', 4.3434 ")
        # pp(parsed_otl)
        parsed_otl = translate_otl("| otstats index=asdf | otstats index='asdsdf' | otstats index=3 | otstats index=4.234")
        # pp(parsed_otl)




