from rest.test import TestCase
from py_otl_parser import Parser
from pprint import pp


class TestSimpleParsing(TestCase):
    def setUp(self):
        self.commands = {
            "readfile": [{"type": "arg"}, {"type": "arg"}, {"type": "arg"}],
            "table": [{"type": "arg"}, {"type": "arg"}, {"type": "arg"}, {"type": "kwarg"}],
            "join": [{"type": "subsearch"}, ],
            "test_sum": [{"type": "arg"}, {"type": "arg"}, ],
            "async": [{"type": "kwarg"}, {"type": "subsearch"}, ],
            "await": [{"type": "kwarg"}, {"type": "subsearch"}, ],

        }
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
        parsed_otl = self.parser.parse("async name='s3', [|readfile 1, 2, 3 ]| table a, b, c, key=val \n| join [| test_sum a, b | await name='s3', [| readfile 1, 2, 3]]", self.commands)
        print(type(parsed_otl))
        pp(parsed_otl)


