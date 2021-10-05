from otlang.otl import OTL
from otl_interpreter.interpreter_db import node_commands_manager


def translate_otl(otl_query):
    command_syntax = node_commands_manager.get_commands_syntax()
    o = OTL(commands=command_syntax)
    return o.translate(otl_query)
