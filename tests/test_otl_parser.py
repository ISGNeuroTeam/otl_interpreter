from rest.test import TestCase
from otlang.otl import OTL
from otlang.sdk.argument import Command
from otl_interpreter.interpreter_db import node_commands_manager
from pprint import pp

from register_test_commands import register_test_commands


class TestSimpleParsing(TestCase):
    def setUp(self):
        # database created after import
        # so we need import translate_otl after test database creations
        from otl_interpreter.translator import translate_otl
        self.translate_otl = translate_otl

        register_test_commands()

    def test_simple_parse(self):
        otl_query = "| readfile arg1, arg2, arg3"

        parsed_otl = self.translate_otl(otl_query)
        self.assertIsInstance(parsed_otl, list)
        command = parsed_otl[0]
        self.assertIsInstance(command, Command)
        self.assertEqual(command.name, 'readfile')
        self.assertEqual(len(command.arguments), 3)
        self.assertEqual(command.arguments['file1'][0].value, 'arg1')
        self.assertEqual(command.arguments['file2'][0].value, 'arg2')
        self.assertEqual(command.arguments['file3'][0].value, 'arg3')


    def test_parse_with_syntax_error(self):
        with self.assertRaises(SyntaxError):
            self.translate_otl("| readfile2 arg3")

    def test_subsearch(self):
        parsed_otl = self.translate_otl("async name=\"s3\", [ | readfile 1, 2, 3] | table a, b, c, key=val \n| join [| sum a, b | await name='s3', override=True]")

    # def test_expression(self):
    #     parsed_otl = self.translate_otl("readfile 1+2, 34+2, 34+23")


    def test_print_subsearches(self):
        parsed_otl = self.translate_otl("| sum arg1, 3, 'asdf', 4.3434 ")
        # pp(parsed_otl)
        parsed_otl = self.translate_otl("| otstats index=asdf | otstats index='asdsdf' | otstats index=3 | otstats index=4.234")
        # pp(parsed_otl)

    def test_register_garbage(self):
        with self.assertRaises(KeyError):
            o = OTL({
                "join": {"rules": [

                    {"type3": "asdf"},
                ]}, })

    def test_command_without_arguments(self):
        test_otl = '| pp_command1 | pp_command1 1'
        with self.assertRaises(SyntaxError):
            parsed_otl = self.translate_otl(test_otl)
