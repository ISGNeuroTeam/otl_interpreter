class Command:
    def __init__(self, name=None, args=None):
        self.name = name or None
        self.args = args or []

    @staticmethod
    def make_command_from_translated_otl(translated_command):
        name = Command._get_name_from_translated_otl(translated_command)
        args = Command._get_args_from_translated_otl(translated_command)
        command = Command(name, args)
        return command

    @staticmethod
    def _get_name_from_translated_otl(translated_command):
        return translated_command['command']['value']

    @staticmethod
    def _get_args_from_translated_otl(translated_command):
        return translated_command['commandargs']

    @staticmethod
    def _get_kwarg(translated_command_arg):
        return Arg(
            arg_type=ArgType.NAMED,
            # TODO make data type enum and define available datatypes
            arg_data_type=translated_command_arg['value']['value']['type'],
            arg_value=translated_command_arg['value']['value']['value'],
            arg_name=translated_command_arg['key']['value'],
        )

    @staticmethod
    def _get_arg(translated_command_arg):

        return Arg(
            arg_type=ArgType.POSITIONAL,
            arg_data_type=translated_command_arg['value']['type'],
            arg_value=translated_command_arg['value']['value'],
        )

    @staticmethod
    def _get_as_arg(translated_command_arg):
        #TODO
        pass

    @staticmethod
    def _get_by_arg(translated_command_arg):
        #TODO
        pass




