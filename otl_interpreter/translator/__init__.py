from datetime import datetime
from otlang.otl import OTL
from otl_interpreter.interpreter_db import node_commands_manager


class Translator:
    def __init__(self):
        self._commands_updated_timestamp = datetime(1, 1, 1, 0, 0)
        self.o = None

    def _init_translator_with_commands(self):
        commands = node_commands_manager.get_commands_syntax()
        if self.o is not None:
            del self.o
        self.o = OTL(commands)

    def _init_translator_with_macroses(self):
        # TODO define how to store macroses and how to get them
        pass

    def __call__(self, otl_query):
        if node_commands_manager.commands_were_updated(self._commands_updated_timestamp):
            self._init_translator_with_commands()
            self._commands_updated_timestamp = datetime.now()
        # TODO check macroses the same way
        return self.o.translate(otl_query)


translate_otl = Translator()

